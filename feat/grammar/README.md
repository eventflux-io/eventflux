# EventFlux Rust SQL Grammar Documentation

This directory contains the comprehensive SQL grammar documentation for EventFlux Rust.

---

## 📖 Single Source of Truth

### **GRAMMAR.md** ⭐

**The complete and authoritative reference** for EventFlux Rust SQL grammar implementation.

**Contains Everything**:
- ✅ Current implementation status (M1 100% complete)
- ✅ All implemented features with working examples
- ✅ Complete SQL syntax reference
- ✅ Architecture and design decisions
- ✅ Future roadmap (Phases 2, 3, 4)
- ✅ Migration guide from old EventFluxQL
- ✅ Performance characteristics
- ✅ Test results (675 passing, 74 ignored)

**When to Read**:
- Want to know what's implemented? → See [Current Status](GRAMMAR.md#current-status)
- Need SQL syntax examples? → See [What's Implemented](GRAMMAR.md#whats-implemented)
- Want to use SQL? → See [SQL Syntax Reference](GRAMMAR.md#sql-syntax-reference)
- Need to understand architecture? → See [Architecture & Design](GRAMMAR.md#architecture--design)
- Curious about design choices? → See [Design Decisions](GRAMMAR.md#design-decisions)
- Planning future work? → See [Future Roadmap](GRAMMAR.md#future-roadmap)
- Migrating from old syntax? → See [Migration Guide](GRAMMAR.md#migration-guide)

---

## Quick Start

### Reading the Documentation

```bash
# View in your browser or editor
cat feat/grammar/GRAMMAR.md

# Or on GitHub
https://github.com/eventflux-io/eventflux/blob/main/feat/grammar/GRAMMAR.md
```

### Using SQL in Code

```rust
use eventflux::core::eventflux_manager::EventFluxManager;

let manager = EventFluxManager::new();

let sql = r#"
    CREATE STREAM StockStream (symbol VARCHAR, price DOUBLE);

    INSERT INTO HighPriceAlerts
    SELECT symbol, price
    FROM StockStream
    WHERE price > 100;
"#;

let runtime = manager
    .create_eventflux_app_runtime_from_string(sql)
    .await?;

runtime.start();
```

---

## Document Status

**Last Updated**: 2025-12-04

**Implementation Status**: ✅ M2 Pattern Processing Complete
- 1,436+ tests passing (370+ pattern processing tests)
- SQL-only production engine
- LALRPOP parser completely removed (December 2024)

**What Changed**:
- ✅ LALRPOP `query_compiler` module completely removed
- ✅ All parsing via vendored `datafusion-sqlparser-rs` (`sql_compiler` module)
- ✅ Single source of truth for all grammar information
- ✅ No legacy parser dependencies

---

## File Structure

```
feat/grammar/
├── README.md     # This file (quick navigation)
└── GRAMMAR.md    # ⭐ Complete SQL grammar reference
```

**Previous files** (now consolidated):
- ~~GRAMMAR_STATUS.md~~ → Merged into GRAMMAR.md
- ~~SQL_IMPLEMENTATION_DESIGN.md~~ → Merged into GRAMMAR.md
- ~~SQL_IMPLEMENTATION_MECHANICS.md~~ → Merged into GRAMMAR.md

---

## Key Highlights

### ✅ What Works Now (M1 Complete)

- **CREATE STREAM** - Define data streams
- **SELECT** - Query and project columns
- **WHERE** - Pre-aggregation filtering
- **Windows** - TUMBLING, SLIDING, LENGTH, LENGTH_BATCH, SESSION
- **Aggregations** - COUNT, SUM, AVG, MIN, MAX
- **Joins** - INNER, LEFT, RIGHT, FULL OUTER
- **GROUP BY** - Grouping and aggregation
- **HAVING** - Post-aggregation filtering
- **ORDER BY** - Sorting results
- **LIMIT/OFFSET** - Pagination
- **INSERT INTO** - Dynamic output streams

### 🔄 Coming Next (Phase 2)

- **DEFINE AGGREGATION** - Incremental aggregations
- **PARTITION** - Partitioning syntax
- **DEFINE FUNCTION** - User-defined functions
- **Pattern Matching** - Sequence detection

---

## Quick Reference Links

| Topic | Link |
|-------|------|
| **Current Status** | [GRAMMAR.md#current-status](GRAMMAR.md#current-status) |
| **Implemented Features** | [GRAMMAR.md#whats-implemented](GRAMMAR.md#whats-implemented) |
| **SQL Syntax** | [GRAMMAR.md#sql-syntax-reference](GRAMMAR.md#sql-syntax-reference) |
| **Architecture** | [GRAMMAR.md#architecture--design](GRAMMAR.md#architecture--design) |
| **Design Decisions** | [GRAMMAR.md#design-decisions](GRAMMAR.md#design-decisions) |
| **Future Roadmap** | [GRAMMAR.md#future-roadmap](GRAMMAR.md#future-roadmap) |
| **Migration Guide** | [GRAMMAR.md#migration-guide](GRAMMAR.md#migration-guide) |

---

**For the complete reference, see [GRAMMAR.md](GRAMMAR.md)**
