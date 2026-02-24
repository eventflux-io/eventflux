# Writing Extensions

This guide explains how to extend the EventFlux Rust runtime with custom
components.  Extensions are registered with a `EventFluxManager` and become
available to applications via annotations like `@source`, `@sink` or `@store`.

## Table Extensions

A table extension implements the `Table` trait and provides a corresponding
`TableFactory`.  The factory is registered under a unique name:

```rust
use eventflux::core::extension::TableFactory;
use eventflux::core::table::Table;

pub struct MyTable;
impl Table for MyTable { /* ... */ }

#[derive(Clone)]
pub struct MyTableFactory;
impl TableFactory for MyTableFactory {
    fn name(&self) -> &'static str { "myStore" }
    fn create(
        &self,
        name: String,
        props: std::collections::HashMap<String, String>,
        ctx: std::sync::Arc<eventflux::core::config::eventflux_context::EventFluxContext>,
    ) -> Result<std::sync::Arc<dyn Table>, String> {
        Ok(std::sync::Arc::new(MyTable))
    }
    fn clone_box(&self) -> Box<dyn TableFactory> { Box::new(self.clone()) }
}
```

After registering the factory with a `EventFluxManager`, applications can define a
table using `@store(type='myStore')`.

### Compiled Conditions

To execute query conditions efficiently, tables can translate expressions into a
custom representation by implementing `compile_condition` and
`compile_update_set`.  The returned structures implement the marker traits
`CompiledCondition` and `CompiledUpdateSet` which the runtime passes back to the
same table instance for `find`, `update` and `delete` operations.

A table may also implement `compile_join_condition` to optimise stream-table
joins.  The provided `JdbcTable` demonstrates translating expressions into SQL
for execution inside a database, while `CacheTable` performs in-memory matching
using compiled values.

Extensions for other storage engines can follow these examples to provide their
own compiled condition formats.

## Dynamic Extensions

Extensions can also be distributed as separate crates compiled into dynamic
libraries.  When the `EventFluxManager` loads a library via `set_extension` it will
try to invoke a set of registration callbacks if they are exported:

```text
register_extension
register_windows
register_functions
register_sources
register_sinks
register_stores
register_source_mappers
register_sink_mappers
```

Each callback should have the type `unsafe extern "C" fn(&EventFluxManager)` and
can register any number of factories.  Only the callbacks required by your
extension need to be implemented.  A minimal crate might look like:

```rust
use eventflux::core::eventflux_manager::EventFluxManager;

#[no_mangle]
pub extern "C" fn register_functions(manager: &EventFluxManager) {
    manager.add_scalar_function_factory("myFn".to_string(), Box::new(MyFn));
}

#[no_mangle]
pub extern "C" fn register_windows(manager: &EventFluxManager) {
    manager.add_window_factory("myWindow".to_string(), Box::new(MyWindowFactory));
}
```

Compile the crate as a `cdylib` and load it at runtime:

```bash
cargo build -p my_extension
```

```rust
manager.set_extension("ext", "./target/debug/libmy_extension.so".into()).unwrap();
```
