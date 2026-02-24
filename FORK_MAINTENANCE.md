# Fork Maintenance Strategy

**Fork**: Apache DataFusion sqlparser-rs v0.59
**Repository**: https://github.com/apache/datafusion-sqlparser-rs
**Our Branch**: `eventflux-extensions`
**Location**: `vendor/datafusion-sqlparser-rs`
**Last Sync**: 2025-10-08 (initial fork)

---

## Why We Fork

We need streaming SQL extensions that are not in standard SQL:
- `WINDOW('type', params)` syntax for streaming windows
- Future: `PARTITION BY` for stream partitioning
- Future: Additional streaming-specific constructs

These extensions are EventFlux-specific and unlikely to be accepted upstream.

---

## Our Extensions

### 1. StreamingWindowSpec Enum
**File**: `vendor/datafusion-sqlparser-rs/src/ast/query.rs` (lines 3751-3796)

```rust
pub enum StreamingWindowSpec {
    Tumbling { duration: Expr },
    Sliding { size: Expr, slide: Expr },
    Length { size: Expr },
    Session { gap: Expr },
    Time { duration: Expr },
    TimeBatch { duration: Expr },
    LengthBatch { size: Expr },
    ExternalTime { timestamp_field: Expr, duration: Expr },
    ExternalTimeBatch { timestamp_field: Expr, duration: Expr },
}
```

### 2. TableFactor Extension
**File**: `vendor/datafusion-sqlparser-rs/src/ast/query.rs` (line 1242)

```rust
pub enum TableFactor {
    Table {
        // ... existing fields
        window: Option<StreamingWindowSpec>,  // EventFlux extension
    },
    // ... other variants
}
```

### 3. Parser Implementation
**File**: `vendor/datafusion-sqlparser-rs/src/parser/mod.rs` (lines 13908-13977)

```rust
fn parse_streaming_window_spec(&mut self) -> Result<StreamingWindowSpec, ParserError> {
    // Parses: WINDOW('type', param1, param2, ...)
}
```

### 4. Public Export
**File**: `vendor/datafusion-sqlparser-rs/src/ast/mod.rs` (line 88)

```rust
pub use self::query::{
    // ... existing exports
    StreamingWindowSpec,  // EventFlux export
    // ...
};
```

### 5. Test Helper Updates
**File**: `vendor/datafusion-sqlparser-rs/src/test_utils.rs` (multiple locations)
- Added `window: None` to TableFactor construction helpers

---

## Sync Schedule

### Regular Maintenance
- **Check upstream**: Every Monday (automated)
- **Sync fork**: Every 3 months (quarterly)
- **Emergency sync**: Immediately for security issues

### Quarterly Sync Process

1. **Preparation**
   ```bash
   cd vendor/datafusion-sqlparser-rs
   git fetch upstream
   git log --oneline HEAD..upstream/main | wc -l  # Check commits behind
   ```

2. **Create Sync Branch**
   ```bash
   git checkout eventflux-extensions
   git checkout -b sync-upstream-vX.XX
   ```

3. **Merge Upstream**
   ```bash
   git remote add upstream https://github.com/apache/datafusion-sqlparser-rs
   git fetch upstream
   git merge upstream/main
   ```

4. **Resolve Conflicts**
   - Focus on our extension files (listed above)
   - Preserve our StreamingWindowSpec additions
   - Update if AST structures changed

5. **Test**
   ```bash
   cd ../../  # Back to eventflux root
   cargo clean
   cargo test
   ```

6. **Merge to Main Extension Branch**
   ```bash
   cd vendor/datafusion-sqlparser-rs
   git checkout eventflux-extensions
   git merge sync-upstream-vX.XX
   git branch -d sync-upstream-vX.XX
   ```

7. **Document**
   - Update this file with new version
   - Add entry to FORK_SYNC_LOG.md
   - Note any breaking changes

---

## Automated Monitoring

### GitHub Action (Recommended)

Create `.github/workflows/check-upstream-fork.yml`:

```yaml
name: Check Fork Upstream Updates
on:
  schedule:
    - cron: '0 0 * * 1'  # Every Monday at midnight
  workflow_dispatch:

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
        with:
          submodules: recursive

      - name: Check upstream versions
        run: |
          cd vendor/datafusion-sqlparser-rs
          git remote add upstream https://github.com/apache/datafusion-sqlparser-rs || true
          git fetch upstream
          BEHIND=$(git rev-list --count HEAD..upstream/main)
          echo "Fork is $BEHIND commits behind upstream"

          if [ "$BEHIND" -gt 100 ]; then
            echo "::warning::Fork is more than 100 commits behind upstream - sync recommended"
          fi
```

---

## Conflict Resolution Strategy

### If Upstream Changes TableFactor

1. Check if they added new fields to `TableFactor::Table`
2. Update our `window` field position if needed
3. Update all pattern matches in:
   - `src/ast/spans.rs`
   - `src/parser/mod.rs`
   - `src/test_utils.rs`

### If Upstream Changes Parser Structure

1. Review parser changes in upstream
2. Update our `parse_streaming_window_spec()` to match patterns
3. Ensure we still parse correctly after their changes

### If Upstream Changes AST Exports

1. Check if they reorganized `ast/mod.rs`
2. Ensure `StreamingWindowSpec` is still exported
3. Update EventFlux imports if needed

---

## Escape Plans

### Plan A: Continue with Fork (Preferred)
- **When**: Syncing is manageable (<2 hours quarterly)
- **Effort**: Low
- **Risk**: Low

### Plan B: Propose Upstream Contribution
- **When**: Our extensions are stable and well-tested
- **Action**: Open RFC with DataFusion community
- **Timeline**: 6-12 months after fork
- **Benefit**: No more fork maintenance

### Plan C: Switch to Different Parser
- **When**: Fork becomes unmaintainable (>8 hours quarterly)
- **Options**:
  - Custom hand-written parser for WINDOW clause
  - Different SQL parser library
  - LALRPOP-based custom parser
- **Effort**: High (4-6 weeks)
- **Risk**: Medium

### Plan D: Revert to Regex Preprocessing
- **When**: All other options fail
- **Action**: Restore `src/sql_compiler/preprocessor.rs`
- **Effort**: Low (1 day)
- **Risk**: Medium (fragile parsing)

**Current Status**: Plan A is working well, no need to switch.

---

## Version Tracking

### Fork History

| Date | Upstream Version | Our Version | Commits Behind | Sync Effort | Notes |
|------|-----------------|-------------|----------------|-------------|-------|
| 2025-10-08 | v0.59 | v0.59+eventflux | 0 | N/A | Initial fork |

---

## Contact

**Maintainer**: EventFlux Core Team
**Questions**: Open GitHub issue with `[fork]` prefix
**Emergency**: If security issue in upstream, sync immediately

---

## Success Metrics

### Fork is Healthy If:
- ✅ Less than 200 commits behind upstream
- ✅ Quarterly sync takes <4 hours
- ✅ No merge conflicts in our extension files
- ✅ All EventFlux tests pass after sync

### Consider Escape Plan If:
- ❌ More than 500 commits behind
- ❌ Sync takes >8 hours
- ❌ Frequent conflicts in extension files
- ❌ Breaking changes every upstream release

**Current Status**: ✅ Fork is healthy

---

**Last Updated**: 2025-10-08
**Next Sync Due**: 2026-01-08 (3 months)
