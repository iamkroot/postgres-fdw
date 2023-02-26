PGX_HOME := ${PWD}/.pgx
PG_DIR := /postgres/build/postgres

# workaround to make sure cargo is found
export PATH := $(PATH):${HOME}/.cargo/bin

install:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
	cargo install --locked cargo-pgx --version 0.6.1
	PGX_HOME=$(PGX_HOME) cargo pgx init --pg15 $(PG_DIR)
	cp data/* $(PG_DIR)/share/extension/
	PGX_HOME=$(PGX_HOME) cargo pgx package --package db721_fdw -c pg_config --out-dir ./out || true
	cp ./out/**/*.so $(PG_DIR)/lib

clean:
	rm -rf ./out
	rm -rf $(PGX_HOME)
