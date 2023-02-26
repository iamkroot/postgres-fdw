# CMU DB 721 Foreign Data Wrapper
Written in Rust using [Supabase Wrappers](https://github.com/supabase/wrappers).

### Setup schema
Adapted from [chicken_farm_schema.sql](https://github.com/cmu-db/postgres/blob/bab87667d83e56fb8a6c01daed81a2c8af7095ad/cmudb/extensions/db721_fdw/chicken_farm_schema.sql) on cmudb's fork and [helloworld_fdw](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/helloworld_fdw/README.md).

```sql
CREATE TABLE IF NOT EXISTS farm
(
    farm_name       varchar,
--     sexes         varchar[],
    min_age_weeks   float4,
    max_age_weeks   float4
);

CREATE TABLE IF NOT EXISTS chicken
(
    identifier      int,
    farm_name       varchar,
    weight_model    varchar,
    sex             varchar,
    age_weeks       float4,
    weight_g        float4,
    notes           varchar
);

\COPY farm FROM '$PWD/data/data-farms.csv' CSV HEADER;
COPY chicken FROM '$PWD/data/data-chickens.csv' CSV HEADER;

CREATE EXTENSION IF NOT EXISTS db721_fdw;

-- create foreign data wrapper and enable 'DB721Fdw'
CREATE FOREIGN DATA WRAPPER db721_wrapper
  HANDLER db721_fdw_handler
  VALIDATOR db721_fdw_validator;

-- create server
CREATE SERVER IF NOT EXISTS db721_server FOREIGN DATA WRAPPER db721_wrapper;


CREATE FOREIGN TABLE IF NOT EXISTS db721_farm
(
    farm_name       varchar,
--     sexes           varchar[],
    min_age_weeks   float4,
    max_age_weeks   float4
) SERVER db721_server OPTIONS
(
    filename '$PWD/data/data-farms.db721',
    tablename 'Farm'
);
CREATE FOREIGN TABLE IF NOT EXISTS db721_chicken (
    identifier      int,
    farm_name       varchar,
    weight_model    varchar,
    sex             varchar,
    age_weeks       float4,
    weight_g        float4,
    notes           varchar
) SERVER db721_server OPTIONS
(
    filename '$PWD/data/data-chickens.db721',
    tablename 'Chicken'
);
```

Note- if the extension `wrappers` exists already, `pgx` will not create new entities that were created in the meantime. In my case, after installing the `helloworld_fdw` wrapper, the various functions of `db721_fdw` were not being populated by pgx inside sql. I had to manually inspect `$HOME/.pgx/15.2/pgx-install/share/postgresql/extension/wrappers--0.1.8.sql` and run the following to load them-
```sql
-- src/fdw/db721_fdw/db721_fdw.rs:5
-- wrappers::fdw::db721_fdw::db721_fdw::__db721_fdw_pgx::db721_fdw_validator
CREATE  FUNCTION "db721_fdw_validator"(
	"options" TEXT[], /* alloc::vec::Vec<core::option::Option<alloc::string::String>> */
	"catalog" Oid /* core::option::Option<u32> */
) RETURNS void
LANGUAGE c /* Rust */
AS '$libdir/wrappers-0.1.8', 'db721_fdw_validator_wrapper';

-- src/fdw/db721_fdw/db721_fdw.rs:5
-- wrappers::fdw::db721_fdw::db721_fdw::__db721_fdw_pgx::db721_fdw_meta
CREATE  FUNCTION "db721_fdw_meta"() RETURNS TABLE (
	"name" TEXT,  /* core::option::Option<alloc::string::String> */
	"version" TEXT,  /* core::option::Option<alloc::string::String> */
	"author" TEXT,  /* core::option::Option<alloc::string::String> */
	"website" TEXT  /* core::option::Option<alloc::string::String> */
)
STRICT
LANGUAGE c /* Rust */
AS '$libdir/wrappers-0.1.8', 'db721_fdw_meta_wrapper';

-- src/fdw/db721_fdw/db721_fdw.rs:5
-- wrappers::fdw::db721_fdw::db721_fdw::__db721_fdw_pgx::db721_fdw_handler
CREATE  FUNCTION "db721_fdw_handler"() RETURNS fdw_handler /* pgx::pgbox::PgBox<pgx_pg_sys::pg15::FdwRoutine> */
STRICT
LANGUAGE c /* Rust */
AS '$libdir/wrappers-0.1.8', 'db721_fdw_handler_wrapper';

```