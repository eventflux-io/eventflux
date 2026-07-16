# Contributing to EventFlux Engine

## Issue First

Every new PR that introduces new functionality must link to an approved issue.
PRs without one may be closed at the maintainer's discretion.

1. Create an issue or comment under an existing one
2. Wait for maintainer approval (`good-first-issue` label or comment)
3. Then code

## Size Limits

For new contributors, keep PRs under 500 lines of code unless explicitly approved by a maintainer under the linked issue.

## High-Risk Areas

These require design discussion in the issue before coding:

- **SQL Parser** — streaming extensions, EventFluxQL grammar
- **Window Processors** — event buffering, expiry, state
- **State Management** — checkpointing, recovery, WAL
- **Distributed Processing** — cluster coordination, transport
- **Event Pipeline** — crossbeam queues, backpressure, object pools
- **Expression Executors** — type coercion, evaluation logic

## PR Requirements

### Run It Locally

**If you can't run it, you can't submit it.**

Authors of PRs must run the code locally. "Relying on CI" is not acceptable.

Clone with submodules — the SQL parser is a vendored submodule and the build fails without it:

```bash
git clone --recursive <your-fork-url>
# or, if already cloned:
git submodule update --init --recursive
```

See [DEV_GUIDE.md](DEV_GUIDE.md#git-submodules-required) for details.

### Single Purpose

One PR = one thing. Bug fix, refactor, feature — separate PRs. Mixed PRs will be closed.

### Quality Checks

All checks must pass before submitting. Connector code is feature-gated, so
lint/test commands use `--features connectors-all` — without it, gated code is
silently skipped:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --features connectors-all -- -D warnings -A clippy::nursery
typos
cargo machete
cargo sort --workspace --check
taplo fmt --check
cargo nextest run --features connectors-all        # or: cargo test --features connectors-all
cargo nextest run                                  # minimal build tests (no connectors)
cargo test --doc --features connectors-all
cargo check --all-targets                          # minimal baseline compiles
cargo check --all-targets --features http          # each connector alone
cargo check --all-targets --features kafka
cargo check --all-targets --features rabbitmq
cargo check --all-targets --features websocket
```

Or run everything at once with [just](https://github.com/casey/just):

```bash
just check-all
```

To auto-fix formatting and typos:

```bash
cargo fmt --all
typos --write-changes
taplo fmt
```

### Typos

We use [typos](https://github.com/crate-ci/typos) for spell checking:

```bash
cargo install typos-cli --locked
typos
```

If a word is a valid domain term (e.g., SQL keywords, proper nouns), add an exception in `.typos.toml`.

### Pre-commit Hooks

We use [pre-commit](https://pre-commit.com/) to run quality checks automatically:

```bash
pip install pre-commit
pre-commit install --install-hooks
```

This installs git hooks that run fast checks (fmt, sort, typos, taplo) on every commit and slow checks (clippy, machete) on push. To run all hooks manually:

```bash
pre-commit run --all-files
```

### Test Runner

We use [cargo-nextest](https://nexte.st/) for faster, parallelized test execution:

```bash
cargo install cargo-nextest
cargo nextest run                          # run all tests
cargo nextest run --nocapture -- test_name # run specific test with output
cargo test --doc                           # doctests (nextest doesn't run these)
```

## Code Style

### Comments: WHY, Not WHAT

```rust
// Bad: Increment counter
counter += 1;

// Good: Offset by 1 because stream event indices are 0-based but IDs are 1-based
counter += 1;
```

Don't comment obvious code. Do explain non-obvious decisions, invariants, and constraints.

### Commit Messages

- Start with a capital letter
- Use imperative mood ("Add feature" not "Added feature")
- Keep under 60 characters
- Do not mention AI assistance

**Good examples:**

```
Add length batch window processor
Fix float literal parsing in WINDOW clause
Optimize event pipeline with lock-free queues
Update Redis state backend configuration
```

## Close Policy

PRs may be closed if:

- No approved issue or no approval from a maintainer
- Code not run and tested locally
- Mixed purposes or purposes not clear
- Can't answer questions about the change
- Inactivity for longer than 7 days

## Questions?

- [GitHub Issues](https://github.com/eventflux-io/eventflux/issues)
- [GitHub Discussions](https://github.com/eventflux-io/eventflux/discussions)
