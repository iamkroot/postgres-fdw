/* 
This file is auto generated by pgx.

The ordering of items is not stable, it is driven by a dependency graph.
*/

-- db721_fdw/src/db721_fdw.rs:11
-- db721_fdw::db721_fdw::__db721_fdw_pgx::db721_fdw_validator
CREATE  FUNCTION "db721_fdw_validator"(
	"options" TEXT[], /* alloc::vec::Vec<core::option::Option<alloc::string::String>> */
	"catalog" Oid /* core::option::Option<u32> */
) RETURNS void
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'db721_fdw_validator_wrapper';

-- db721_fdw/src/db721_fdw.rs:11
-- db721_fdw::db721_fdw::__db721_fdw_pgx::db721_fdw_meta
CREATE  FUNCTION "db721_fdw_meta"() RETURNS TABLE (
	"name" TEXT,  /* core::option::Option<alloc::string::String> */
	"version" TEXT,  /* core::option::Option<alloc::string::String> */
	"author" TEXT,  /* core::option::Option<alloc::string::String> */
	"website" TEXT  /* core::option::Option<alloc::string::String> */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'db721_fdw_meta_wrapper';

-- db721_fdw/src/db721_fdw.rs:11
-- db721_fdw::db721_fdw::__db721_fdw_pgx::db721_fdw_handler
CREATE  FUNCTION "db721_fdw_handler"() RETURNS fdw_handler /* pgx::pgbox::PgBox<pgx_pg_sys::pg15::FdwRoutine> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'db721_fdw_handler_wrapper';

-- create foreign data wrapper and enable 'DB721Fdw'
CREATE FOREIGN DATA WRAPPER db721_fdw
  HANDLER db721_fdw_handler
  VALIDATOR db721_fdw_validator;