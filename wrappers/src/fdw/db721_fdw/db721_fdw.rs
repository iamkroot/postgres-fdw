use std::collections::HashMap;
use std::os::unix::prelude::FileExt;

use pgx::PgSqlErrorCode;
use supabase_wrappers::prelude::*;

use super::metadata::{Metadata, Stats};
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

struct Db721Reader {
    file: std::fs::File,
    metadata: Metadata,

    // query specific
    cols: Vec<String>,
    limit: Limit,
    _quals: Vec<Qual>,

    // scan state
    row_cnt: i64,
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
        Ok(Self {
            file: db721_file.file,
            metadata: db721_file.metadata,
            cols: cols.to_vec(),
            limit,
            _quals: quals.to_vec(),
            row_cnt: 0,
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
        // this is called on each row and we only return one row here
        let Some(reader) = self.reader.as_mut() else {
            return None;
        };
        if reader.row_cnt >= reader.limit.count {
            return None;
        }
        for colname in &reader.cols {
            let Some(col) = reader.metadata.columns.get(colname) else {
                return None;
            };

            match &col.block_stats {
                Stats::Float(_) => {
                    const FIELD_SIZE: usize = 4;
                    let read_offset = col.start_offset as i64 + FIELD_SIZE as i64 * reader.row_cnt;
                    let mut buf = [0; FIELD_SIZE];
                    if let Err(err) = reader.file.read_exact_at(&mut buf, read_offset as u64) {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("error reading f32 at offset {read_offset} bytes: {err}"),
                        );
                        return None;
                    };
                    let f = f32::from_le_bytes(buf);
                    log::trace!(target: "db721_read", "{colname} float read offset {read_offset} {buf:?} {f}");
                    row.push(colname, Some(Cell::F32(f)));
                }
                Stats::Int(_) => {
                    const FIELD_SIZE: usize = 4;
                    let read_offset = col.start_offset as i64 + FIELD_SIZE as i64 * reader.row_cnt;
                    let mut buf = [0; FIELD_SIZE];
                    if let Err(err) = reader.file.read_exact_at(&mut buf, read_offset as u64) {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("error reading i32 at offset {read_offset} bytes: {err}"),
                        );
                        return None;
                    };
                    log::trace!(target: "db721_read", "{colname} int read offset {read_offset} {buf:?}");
                    row.push(colname, Some(Cell::I32(i32::from_le_bytes(buf))));
                }
                Stats::Str(_) => {
                    const FIELD_SIZE: usize = 32;
                    let read_offset = col.start_offset as i64 + FIELD_SIZE as i64 * reader.row_cnt;
                    let mut buf = [0; FIELD_SIZE];
                    if let Err(err) = reader.file.read_exact_at(&mut buf, read_offset as u64) {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("error reading str at offset {read_offset} bytes: {err}"),
                        );
                        return None;
                    };
                    log::trace!(target: "db721_read", "{colname} str read offset {read_offset} {buf:?}");
                    let null_pos = buf.iter().position(|c| *c == 0).expect("No null char");
                    row.push(
                        colname,
                        Some(Cell::PgString(PgString::from_slice(&buf[..null_pos + 1]))),
                    );
                }
            }
        }
        reader.row_cnt += 1;
        Some(())
    }

    fn end_scan(&mut self) {
        self.reader.take();
        // we do nothing here, but you can do things like resource cleanup and etc.
    }
}
