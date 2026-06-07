alias b  := build
alias t  := test
alias n  := nextest

build:
  cargo build

test: build
  cargo test

# Run a specific test by name
tests TEST: build
  cargo test {{TEST}}

nextest: build
  cargo nextest run

# Run with output visible
nextests TEST: build
  cargo nextest run --nocapture -- {{TEST}}

server *ARGS:
  cargo run --bin run_eventflux {{ARGS}}

# Code quality
lint:
  cargo clippy --all-targets -- -D warnings -A clippy::nursery

fmt:
  cargo fmt --all

fmt-check:
  cargo fmt --all -- --check

sort:
  cargo sort --workspace

machete:
  cargo machete

# TOML formatting
taplo:
  taplo fmt

taplo-check:
  taplo fmt --check

# Spelling check
typos:
  typos

# Code coverage
coverage:
  cargo llvm-cov --all-features --lcov --output-path lcov.info

# All quality checks (run before PR)
check-all: fmt sort taplo lint
  cargo machete
  typos

# Generate changelog
changelog:
  git cliff -o CHANGELOG.md

# Generate release notes for latest version
release-notes:
  git cliff --latest -o RELEASE_NOTES.md
