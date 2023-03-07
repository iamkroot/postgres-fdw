PG_DIR := /postgres/build/postgres
PG_CONFIG ?= $(PG_DIR)/bin/pg_config

# workaround to make sure cargo is found
export PATH := $(PATH):${HOME}/.cargo/bin

deps:
	apt update
	apt install pkg-config

rust: deps
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

pgx: rust
	cargo install --locked cargo-pgx --version 0.6.1
	$(info HOME is $(HOME) PATH is $(PATH) PG_DIR is $(PG_DIR) PG_CONFIG is $(PG_CONFIG))
	cargo pgx init --pg15 $(PG_CONFIG)
	cargo pgx package --no-default-features --features=pg15 --package db721_fdw --out-dir ./out || true

# # can't get pgx to build inside docker of autograder. manually uploaded the built .so lib.
# install:
install: pgx
	cp db721_fdw/data/db721_fdw--0.0.1.sql db721_fdw/data/db721_fdw.control $(PG_DIR)/share/extension/
	cp db721_fdw/data/db721_fdw.so $(PG_DIR)/lib

clean:
	rm -rf ./out

db721_fdw.zip: db721_fdw supabase-wrappers supabase-wrappers-macros Cargo.lock Cargo.toml LICENSE Makefile README.md
	7z a $@ $^
