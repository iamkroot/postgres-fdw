use std::collections::HashMap;
use std::os::unix::prelude::FileExt;

use pgx::PgSqlErrorCode;
use supabase_wrappers::prelude::*;

use super::metadata::Stats;
use super::parser::{parse_file, Db721File};

// A simple demo FDW
#[wrappers_fdw(
    version = "0.1.0",
    author = "iamkroot",
    website = "https://github.com/iamkroot/postgres-fdw/tree/db721/wrappers/src/fdw/db721_fdw"
)]
pub(crate) struct Db721Fdw {
    // row counter
    row_cnt: u64,

    // target column name list
    tgt_cols: Vec<String>,
    db721_file: Option<Db721File>,
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

        Self {
            row_cnt: 0,
            tgt_cols: Vec::new(),
            db721_file: None,
        }
    }

    fn begin_scan(
        &mut self,
        _quals: &[Qual],
        columns: &[String],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) {
        log::trace!("scan options: {options:?}");

        let Some(filename) = require_option("filename", options) else {
            return;
        };
        let db721_file = match parse_file(&filename) {
            Ok(f) => f,
            Err(err) => {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    &format!("parse of DB721 file at {filename} failed: {err}"),
                );
                return;
            }
        };

        self.db721_file = Some(db721_file);
        // reset row counter
        self.row_cnt = 0;

        // save a copy of target columns
        self.tgt_cols = columns.to_vec();
    }

    fn iter_scan(&mut self, row: &mut Row) -> Option<()> {
        // this is called on each row and we only return one row here
        let Some(db721_file) = self.db721_file.as_mut() else {
            return None;
        };
        if self.row_cnt >= db721_file.metadata.num_rows {
            return None;
        }
        // let tgt_block = (self.row_cnt / (db721_file.metadata.max_vals_per_block as u64)) as u32;
        for colname in &self.tgt_cols {
            let Some(col) = db721_file.metadata.columns.get(colname) else {
                return None;
            };

            match &col.block_stats {
                Stats::Float(_) => {
                    let read_offset = col.start_offset as u64 + 4 * self.row_cnt;
                    let mut buf = [0; 4];
                    if let Err(err) = db721_file.file.read_exact_at(&mut buf, read_offset) {
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
                    let read_offset = col.start_offset as u64 + 4 * self.row_cnt;
                    let mut buf = [0; 4];
                    if let Err(err) = db721_file.file.read_exact_at(&mut buf, read_offset) {
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
                    const FIELD_SIZE: u64 = 32;
                    let read_offset = col.start_offset as u64 + FIELD_SIZE * self.row_cnt;
                    let mut buf = [0; FIELD_SIZE as usize];
                    if let Err(err) = db721_file.file.read_exact_at(&mut buf, read_offset) {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            &format!("error reading f32 at offset {read_offset} bytes: {err}"),
                        );
                        return None;
                    };
                    let null_pos = buf.iter().position(|c| *c == 0).expect("No null char");
                    let cstr = std::ffi::CStr::from_bytes_with_nul(&buf[..null_pos + 1]).unwrap();
                    let val = cstr.to_string_lossy().to_string();
                    row.push(colname, Some(Cell::String(val)));
                }
            }
        }
        self.row_cnt += 1;
        // return 'None' to stop data scan
        Some(())
    }

    fn end_scan(&mut self) {
        self.db721_file.take();
        // we do nothing here, but you can do things like resource cleanup and etc.
    }
}
