use std::collections::HashMap;
use std::str::FromStr;

use pgx::PgSqlErrorCode;
use supabase_wrappers::prelude::*;

use super::metadata::{Column, Metadata, Stats, BSS};
use super::parser::parse_file;

/// FDW for the [DB721 file format](https://15721.courses.cs.cmu.edu/spring2023/project1.html).
#[wrappers_fdw(
    version = "0.1.0",
    author = "iamkroot",
    website = "https://github.com/iamkroot/postgres-fdw/tree/db721/db721_fdw"
)]
pub(crate) struct Db721Fdw {
    reader: Option<Db721Reader>,
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Eq,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl Op {
    /// Returns true if `lhs op rhs` is true.
    fn eval<T: PartialEq + PartialOrd>(&self, lhs: T, rhs: T) -> bool {
        match self {
            Op::Eq => lhs == rhs,
            Op::Lt => lhs < rhs,
            Op::Lte => lhs <= rhs,
            Op::Gt => lhs > rhs,
            Op::Gte => lhs >= rhs,
        }
    }
}

impl FromStr for Op {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "=" => Ok(Self::Eq),
            "<" => Ok(Self::Lt),
            "<=" => Ok(Self::Lte),
            ">" => Ok(Self::Gt),
            ">=" => Ok(Self::Gte),
            // unsupported
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
enum PolyVal {
    Int(i32),
    Float(f32),
    Str(String),
}

impl TryFrom<&Value> for PolyVal {
    type Error = ();

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Cell(Cell::I32(v)) => Ok(PolyVal::Int(*v)),
            Value::Cell(Cell::F32(v)) => Ok(PolyVal::Float(*v)),
            Value::Cell(Cell::F64(v)) => Ok(PolyVal::Float(*v as f32)),
            Value::Cell(Cell::String(v)) => Ok(PolyVal::Str(v.to_owned())),
            _ => Err(()),
        }
    }
}

struct CustomQual {
    op: Op,
    rhs: PolyVal,
}

impl CustomQual {
    /// Evaluate the predicate on the given value.
    /// Return true if `lhs` satisfies the predicate.
    fn eval(&self, lhs: &Cell) -> bool {
        match (lhs, &self.rhs) {
            (Cell::F32(lhs), PolyVal::Float(rhs)) => self.op.eval(*lhs, *rhs),
            (Cell::I32(lhs), PolyVal::Int(rhs)) => self.op.eval(*lhs, *rhs),

            (Cell::PgString(lhs), PolyVal::Str(rhs)) => {
                self.op.eval(lhs.to_slice(), rhs.as_bytes())
            }
            (Cell::String(lhs), PolyVal::Str(rhs)) => self.op.eval(lhs, rhs),
            (lhs, rhs) => {
                report_warning(&format!(
                    "Unsupported data types in predicate! {lhs}, {rhs:?}"
                ));
                false
            }
        }
    }
}

struct Db721Reader {
    mmap: memmap2::Mmap,
    metadata: Metadata,
    num_blocks: u32,

    // query specific
    cols: Vec<String>,
    limit: Limit,
    quals: HashMap<String, (usize, CustomQual)>,
    non_pred_cols: Vec<(usize, String)>,

    // scan state
    row_cnt: i64,
    block_num: u32,
    block_row_num: u32,
}

impl Db721Reader {
    fn new(
        filename: &str,
        cols: &[String],
        quals: &[Qual],
        limit: &Option<Limit>,
    ) -> Result<Self, ()> {
        let db721_file = match parse_file(filename) {
            Ok(f) => f,
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("parse of DB721 file at {filename} failed: {err}"),
                );
                return Err(());
            }
        };
        assert!(cols
            .iter()
            .all(|c| db721_file.metadata.columns.contains_key(c)));
        assert!(quals
            .iter()
            .all(|q| db721_file.metadata.columns.contains_key(&q.field)));
        let num_rows = db721_file.metadata.num_rows() as i64;
        let limit = limit
            .clone()
            .map(|Limit { count, offset }| {
                if offset + count > num_rows {
                    Limit {
                        count: num_rows - offset,
                        offset,
                    }
                } else {
                    Limit { count, offset }
                }
            })
            .unwrap_or_else(|| Limit {
                count: num_rows,
                offset: 0,
            });
        let quals = {
            let mut qs = HashMap::with_capacity(quals.len());
            for q in quals {
                if q.use_or {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("unsupported use_or in qual: {q:?}"),
                    );
                    return Err(());
                }
                let Ok(op) = Op::from_str(&q.operator) else {
                    report_error(PgSqlErrorCode::ERRCODE_FDW_ERROR, &format!("unsupported op in qual: {q:?}"));
                    return Err(());
                };
                let Ok(rhs) = (&q.value).try_into() else {
                    report_error(PgSqlErrorCode::ERRCODE_FDW_ERROR, &format!("unsupported rhs in qual: {q:?}"));
                    return Err(());
                };
                qs.insert(
                    q.field.clone(),
                    (
                        cols.iter().position(|f| f == &q.field).unwrap(),
                        CustomQual { op, rhs },
                    ),
                );
            }
            qs
        };
        let non_pred_cols = cols
            .iter()
            .enumerate()
            .filter_map(|(i, c)| (!quals.contains_key(c)).then(|| (i, c.clone())))
            .collect();
        let num_blocks = db721_file
            .metadata
            .columns
            .values()
            .next()
            .unwrap()
            .num_blocks;
        db721_file
            .mmap
            .advise(memmap2::Advice::Sequential)
            .expect("madvise failed");
        let mut reader = Self {
            mmap: db721_file.mmap,
            metadata: db721_file.metadata,
            num_blocks,
            cols: cols.to_vec(),
            limit,
            quals,
            non_pred_cols,
            row_cnt: 0,
            block_num: 0,
            block_row_num: 0,
        };
        let mut block_num = 0;
        while block_num < num_blocks && reader.skip_block(block_num) {
            block_num += 1;
        }
        if block_num >= num_blocks {
            // filtered out all the rows!
            log::debug!("Filtered out all the rows!");
            return Err(());
        }
        reader.block_num = block_num;
        Ok(reader)
    }

    /// Read the val specified by self.block_row_num
    fn read_cur_val(&self, col: &Column, out: &mut Cell) {
        let abs_row_num = self.metadata.max_vals_per_block * self.block_num + self.block_row_num;
        let read_offset = col.start_offset + abs_row_num * col.field_size();
        match col.block_stats {
            Stats::Float(_) => {
                const FIELD_SIZE: usize = 4;
                let mut buf = [0; FIELD_SIZE];
                buf.copy_from_slice(
                    &self.mmap[read_offset as usize..read_offset as usize + FIELD_SIZE],
                );
                *out = Cell::F32(f32::from_ne_bytes(buf));
                log::trace!(target: "db721_read", "float read offset {read_offset} {buf:?} {out}");
            }
            Stats::Int(_) => {
                const FIELD_SIZE: usize = 4;
                let mut buf = [0; FIELD_SIZE];
                buf.copy_from_slice(
                    &self.mmap[read_offset as usize..read_offset as usize + FIELD_SIZE],
                );
                *out = Cell::I32(i32::from_ne_bytes(buf));
                log::trace!(target: "db721_read", "int read offset {read_offset} {buf:?} {out}");
            }
            Stats::Str(_) => {
                const FIELD_SIZE: usize = 32;
                let buf = &self.mmap[read_offset as usize..read_offset as usize + FIELD_SIZE];
                let null_pos = buf.iter().position(|c| *c == 0).expect("No null char");
                // *out = Cell::String(String::from_utf8_lossy(&buf[..null_pos]).to_string());
                *out = Cell::PgString(PgString::from_slice(&buf[..null_pos]));
                log::trace!(target: "db721_read", "str read offset {read_offset} {buf:?}");
            }
        }
    }

    /// Determine if the block is to be read, skipping over the ones filtered out by predicate pushdown.
    fn skip_block(&self, block_num: u32) -> bool {
        self.quals.iter().any(|(pred_colname, (_, q))| {
            let col = self.metadata.columns.get(pred_colname).unwrap();
            match &col.block_stats {
                Stats::Float(BSS { block_stats }) => {
                    if let Some(stats) = block_stats.get(&block_num) {
                        let PolyVal::Float(rhs) = q.rhs else {
                            panic!()
                        };
                        match q.op {
                            Op::Eq => stats.min > rhs || stats.max < rhs,
                            Op::Lt => stats.min >= rhs,
                            Op::Lte => stats.min > rhs,
                            Op::Gt => stats.max <= rhs,
                            Op::Gte => stats.max < rhs,
                        }
                    } else {
                        // no block stats, can't skip
                        false
                    }
                }
                Stats::Int(BSS { block_stats }) => {
                    if let Some(stats) = block_stats.get(&block_num) {
                        let PolyVal::Int(rhs) = q.rhs else {
                            panic!()
                        };
                        match q.op {
                            Op::Eq => stats.min > rhs || stats.max < rhs,
                            Op::Lt => stats.min >= rhs,
                            Op::Lte => stats.min > rhs,
                            Op::Gt => stats.max <= rhs,
                            Op::Gte => stats.max < rhs,
                        }
                    } else {
                        // no block stats, can't skip
                        false
                    }
                }
                Stats::Str(BSS { block_stats }) => {
                    if let Some(stats) = block_stats.get(&block_num) {
                        let PolyVal::Str(rhs) = &q.rhs else {
                            panic!()
                        };
                        let rhs_len = rhs.len() as u32;
                        match q.op {
                            Op::Eq => {
                                stats.max_len < rhs_len
                                    || stats.min_len > rhs_len
                                    || &stats.min > rhs
                                    || &stats.max < rhs
                            }
                            Op::Lt => &stats.min >= rhs,
                            Op::Lte => &stats.min > rhs,
                            Op::Gt => &stats.max <= rhs,
                            Op::Gte => &stats.max < rhs,
                        }
                    } else {
                        // no block stats, can't skip
                        false
                    }
                }
            }
        })
    }
}

impl ForeignDataWrapper for Db721Fdw {
    fn new(_options: &HashMap<String, String>) -> Self {
        static mut LOG_INIT: bool = false;
        if unsafe { !LOG_INIT } {
            // is there a better way to init the logger?
            env_logger::builder()
                .target(env_logger::Target::Pipe(Box::new(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(concat!(env!("CARGO_MANIFEST_DIR"), "/db721.log"))
                        .unwrap(),
                )))
                .try_init()
                .unwrap_or_else(|e| log::warn!("Failed to init logger {e}"));
            unsafe { LOG_INIT = true };
        }
        log::trace!("init options: {_options:?}");

        Self { reader: None }
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[String],
        _sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        log::trace!("scan options: {options:?}");

        let Some(filename) = require_option("filename", options) else {
            return;
        };
        self.reader = Db721Reader::new(&filename, columns, quals, limit).ok();
    }

    fn iter_scan(&mut self, row: &mut Row) -> Option<()> {
        let reader = self.reader.as_mut()?;
        if reader.row_cnt >= reader.limit.count {
            return None;
        }
        while reader.block_num < reader.num_blocks {
            let num_rows = reader.metadata.num_rows_in_block(reader.block_num);
            log::trace!(target: "exec", "{num_rows} rows in block {}", reader.block_num);
            while reader.block_row_num < num_rows {
                let mut all_passed = true;
                // row.cols is not really needed by postgres, just init it to default.
                row.cols.resize_with(reader.cols.len(), Default::default);
                // init row.cells
                row.cells.resize(reader.cols.len(), Some(Cell::I32(0)));
                for (colname, (i, q)) in &reader.quals {
                    let col = reader.metadata.columns.get(colname).unwrap();
                    let cell = &mut row.cells[*i];
                    let cell = cell.as_mut().unwrap();
                    reader.read_cur_val(col, cell);
                    if !q.eval(&cell) {
                        // row does not statisfy the predicate
                        log::trace!(target: "exec", "val {cell} filtered out");
                        all_passed = false;
                        break;
                    }
                }
                if all_passed {
                    for (i, colname) in &reader.non_pred_cols {
                        let col = reader.metadata.columns.get(colname).unwrap();
                        let cell = &mut row.cells[*i];
                        let cell = cell.as_mut().unwrap();
                        reader.read_cur_val(col, cell);
                    }
                    reader.block_row_num += 1;
                    reader.row_cnt += 1;
                    return Some(());
                } else {
                    reader.block_row_num += 1;
                }
            }
            // end of current block, try next one
            reader.block_row_num = 0;
            reader.block_num += 1;
            while reader.block_num < reader.num_blocks && reader.skip_block(reader.block_num) {
                reader.block_num += 1;
            }
        }
        None
    }

    fn end_scan(&mut self) {
        self.reader.take();
    }
}