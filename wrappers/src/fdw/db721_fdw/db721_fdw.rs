use std::collections::HashMap;
use std::os::unix::prelude::FileExt;
use std::str::FromStr;

use pgx::PgSqlErrorCode;
use supabase_wrappers::prelude::*;

use super::metadata::{Column, Metadata, Stats, BSS};
use super::parser::parse_file;

// A simple demo FDW
#[wrappers_fdw(
    version = "0.1.0",
    author = "iamkroot",
    website = "https://github.com/iamkroot/postgres-fdw/tree/db721/wrappers/src/fdw/db721_fdw"
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
        match lhs {
            Cell::F32(lhs) => {
                let PolyVal::Float(rhs) = self.rhs else {
                    panic!("data type mismatch!");
                };
                self.op.eval(*lhs, rhs)
            }
            Cell::I32(lhs) => {
                let PolyVal::Int(rhs) = self.rhs else {
                    panic!("data type mismatch!");
                };
                self.op.eval(*lhs, rhs)
            }
            Cell::PgString(lhs) => {
                let PolyVal::Str(rhs) = &self.rhs else {
                    panic!("data type mismatch!");
                };
                let res = self.op.eval(lhs.to_slice(), rhs.as_bytes());
                res
            }
            _ => panic!(),
        }
    }
}

struct Db721Reader {
    file: std::fs::File,
    metadata: Metadata,
    num_blocks: u32,

    // query specific
    cols: Vec<String>,
    limit: Limit,
    quals: HashMap<String, CustomQual>,

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
                qs.insert(q.field.clone(), CustomQual { op, rhs });
            }
            qs
        };
        let num_blocks = db721_file
            .metadata
            .columns
            .values()
            .next()
            .unwrap()
            .num_blocks;

        let mut reader = Self {
            file: db721_file.file,
            metadata: db721_file.metadata,
            num_blocks,
            cols: cols.to_vec(),
            limit,
            quals,
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
            log::info!("Filtered out all the rows!");
            return Err(());
        }
        reader.block_num = block_num;
        Ok(reader)
    }

    /// Low-level, read the value from file
    fn read_val(&self, read_offset: u64, res: &mut Cell) -> Option<()> {
        match res {
            Cell::F32(v) => {
                const FIELD_SIZE: usize = 4;
                let mut buf = [0; FIELD_SIZE];
                if let Err(err) = self.file.read_exact_at(&mut buf, read_offset) {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("error reading f32 at offset {read_offset} bytes: {err}"),
                    );
                    return None;
                };
                *v = f32::from_le_bytes(buf);
                log::trace!(target: "db721_read", "float read offset {read_offset} {buf:?} {}", *v);
            }
            Cell::I32(v) => {
                const FIELD_SIZE: usize = 4;
                let mut buf = [0; FIELD_SIZE];
                if let Err(err) = self.file.read_exact_at(&mut buf, read_offset) {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("error reading i32 at offset {read_offset} bytes: {err}"),
                    );
                    return None;
                };
                *v = i32::from_le_bytes(buf);
                log::trace!(target: "db721_read", "int read offset {read_offset} {buf:?} {}", *v);
            }
            Cell::PgString(v) => {
                const FIELD_SIZE: usize = 32;
                let mut buf = [0; FIELD_SIZE];
                if let Err(err) = self.file.read_exact_at(&mut buf, read_offset) {
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        &format!("error reading str at offset {read_offset} bytes: {err}"),
                    );
                    return None;
                };
                log::trace!(target: "db721_read", "str read offset {read_offset} {buf:?}");
                let null_pos = buf.iter().position(|c| *c == 0).expect("No null char");
                *v = PgString::from_slice(&buf[..null_pos]);
            }
            _ => return None,
        }
        Some(())
    }

    /// Read the val specified by self.block_cnt and self.
    fn read_cur_val(&self, col: &Column) -> Option<Cell> {
        let (field_size, mut out) = match &col.block_stats {
            Stats::Float(_) => (4, Cell::F32(0.0)),
            Stats::Int(_) => (4, Cell::F32(0.0)),
            Stats::Str(_) => (32, Cell::PgString(PgString::from_slice(&[]))),
        };
        let abs_row_num = self.metadata.max_vals_per_block * self.block_num + self.block_row_num;
        let read_offset = col.start_offset + abs_row_num * field_size;
        self.read_val(read_offset as u64, &mut out).map(|_| out)
    }

    /// Determine if the block is to be read, skipping over the ones filtered out by predicate pushdown.
    fn skip_block(&self, block_num: u32) -> bool {
        self.quals.iter().any(|(pred_colname, q)| {
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
    // You can do any initalization in this new() function, like saving connection
    // info or API url in an variable, but don't do any heavy works like making a
    // database connection or API call.
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
        let Some(reader) = self.reader.as_mut() else {
            return None;
        };
        if reader.row_cnt >= reader.limit.count {
            return None;
        }
        while reader.block_num < reader.num_blocks {
            let num_rows = reader.metadata.num_rows_in_block(reader.block_num);
            log::trace!(target: "exec", "{num_rows} rows in block {}", reader.block_num);
            while reader.block_row_num < num_rows {
                let mut all_passed = true;
                for colname in &reader.cols {
                    let col = reader.metadata.columns.get(colname).unwrap();
                    let val = reader.read_cur_val(col)?;
                    let qual = reader.quals.get(colname);
                    if !qual.map_or(true, |q| q.eval(&val)) {
                        // row does not statisfy the predicate
                        log::trace!(target: "exec", "val {val} filtered out");
                        row.clear();
                        all_passed = false;
                        break;
                    } else {
                        row.push(colname, Some(val));
                    }
                }
                reader.block_row_num += 1;
                if all_passed {
                    reader.row_cnt += 1;
                    return Some(());
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
