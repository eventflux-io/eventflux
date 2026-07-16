alias b  := build
alias t  := test
alias n  := nextest

# Connector code is feature-gated (rabbitmq, websocket, ...); dev commands
# default to connectors-all so nothing is silently left unbuilt/untested.
# `check-minimal` guards the minimal (no-connector) baseline.

build:
  cargo build --features connectors-all

test: build
  cargo test --features connectors-all

# Run a specific test by name
tests TEST: build
  cargo test --features connectors-all {{TEST}}

nextest: build
  cargo nextest run --features connectors-all

# Run with output visible
nextests TEST: build
  cargo nextest run --features connectors-all --nocapture -- {{TEST}}

server *ARGS:
  cargo run --features connectors-all --bin run_eventflux {{ARGS}}

# Code quality
lint:
  cargo clippy --all-targets --features connectors-all -- -D warnings -A clippy::nursery

# Feature-gating honesty: minimal build and each connector alone must compile
check-minimal:
  cargo check --all-targets
  cargo check --all-targets --features http
  cargo check --all-targets --features kafka
  cargo check --all-targets --features rabbitmq
  cargo check --all-targets --features websocket

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
check-all: fmt sort taplo lint check-minimal
  cargo machete
  typos

# Generate changelog
changelog:
  git cliff -o CHANGELOG.md

# Generate release notes for latest version
release-notes:
  git cliff --latest -o RELEASE_NOTES.md
