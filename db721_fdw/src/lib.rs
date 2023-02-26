#![allow(clippy::module_inception)]
mod db721_fdw;
mod parser;
mod metadata;
use pgx::pg_module_magic;

pg_module_magic!();
