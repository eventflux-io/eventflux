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

### Single Purpose

One PR = one thing. Bug fix, refactor, feature — separate PRs. Mixed PRs will be closed.

### Quality Checks

All checks must pass before submitting:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
typos
cargo machete
cargo sort --workspace --check
cargo test
```

To auto-fix formatting: `cargo fmt --all`

To auto-fix typos: `typos --write-changes`

### Typos

We use [typos](https://github.com/crate-ci/typos) for spell checking:

```bash
cargo install typos-cli --locked
typos
```

If a word is a valid domain term (e.g., SQL keywords, proper nouns), add an exception in `.typos.toml`.

### Pre-commit Hooks

We use [prek](https://github.com/j178/prek) to run quality checks before each commit:

```bash
cargo install prek
prek install
```

This installs git hooks that automatically run fmt, clippy, typos, machete, and sort checks on every commit. To run all hooks manually:

```bash
prek run --all-files
```

> **Note**: `prek` is optional for local development. CI runs the same checks independently. The `.pre-commit-config.yaml` is also compatible with standard [pre-commit](https://pre-commit.com/) if you prefer that tool.

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
