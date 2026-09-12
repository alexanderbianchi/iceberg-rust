Got it. Yes—the larger architectural problem is in **`iceberg-rust/crates/integrations/datafusion`**, not DataFusion Distributed.

After tracing the integration, I think several responsibilities happen one phase too late.

## Current flow

### Catalog metadata is loaded during physical planning

`IcebergTableProvider::scan()` calls:

```rust
self.source.table_for_planning().await
```

For the existing plain provider, that reaches:

```rust
catalog.load_table(table_ident).await
```

So `TableProvider::schema()` may describe metadata loaded at provider construction, while `scan()` may use a newer table.

### Iceberg scan planning happens during execution

`IcebergTableScan::execute()` calls `get_batch_stream()`, which does:

```rust
let table_scan = table.scan().build()?;

table_scan
    .to_arrow()
    .await?
```

`TableScan::to_arrow()` internally calls:

```rust
self.plan_files().await?
```

Therefore manifest reading, pruning, and `FileScanTask` creation occur inside `ExecutionPlan::execute()`, not when the physical plan is created.

The phase boundaries are currently:

```text
TableProvider::scan()
  └── load current table metadata

ExecutionPlan::execute()
  ├── select snapshot
  ├── read manifests
  ├── plan files
  └── read files
```

That explains why it feels wrong.

## Better phase separation

```text
Catalog resolution
  ├── authenticate
  ├── load Table
  └── bind one metadata version

TableProvider::scan()
  ├── select snapshot
  ├── convert predicates
  ├── plan FileScanTasks
  ├── divide tasks into DataFusion partitions
  └── return immutable ExecutionPlan

ExecutionPlan::execute(partition)
  └── read only the preplanned tasks for that partition
```

This would make the physical plan actually describe the work it will execute.

## `IcebergTableScan` problems

In `physical_plan/scan.rs`:

```rust
fn execute(
    &self,
    _partition: usize,
    _context: Arc<TaskContext>,
)
```

Both arguments are ignored.

Consequences:

- The scan advertises one unknown partition.
- It cannot distribute Iceberg files across DataFusion partitions.
- Every execution invocation would plan files again.
- It does not use DataFusion’s `TaskContext` for runtime resources.
- File planning is invisible to DataFusion’s physical optimizer.
- The physical plan is difficult to serialize because it carries an Iceberg `Table`, not explicit work.

A more appropriate plan is:

```rust
struct IcebergScanExec {
    partitions: Vec<Vec<FileScanTask>>,
    schema: SchemaRef,
    properties: PlanProperties,
}
```

Then:

```rust
fn execute(&self, partition: usize, context: Arc<TaskContext>) {
    let tasks = self.partitions[partition].clone();
    // Read exactly those tasks.
}
```

## Is using `ArrowReader` wrong?

Not inherently.

Iceberg’s `ArrowReaderBuilder` already handles Iceberg-specific behavior:

- field-ID projection;
- schema evolution;
- positional/equality deletes;
- row-group pruning;
- encryption;
- Iceberg `FileIO`.

Replacing it immediately with DataFusion’s Parquet reader risks reimplementing Iceberg semantics incorrectly.

The problematic call is specifically:

```rust
TableScan::to_arrow()
```

It combines two separate concerns:

```text
plan_files() + ArrowReaderBuilder::read(...)
```

For DataFusion, we should split them:

### During `TableProvider::scan()`

```rust
let tasks = table_scan.plan_files().await?;
```

Collect and partition those tasks.

### During `ExecutionPlan::execute(partition)`

```rust
ArrowReaderBuilder::new(...)
    .build()
    .read(tasks_for_partition)
```

That retains Iceberg’s reader initially while giving DataFusion an immutable, partitioned plan.

Longer-term, an `IcebergFileSource` integrated with DataFusion’s `DataSourceExec` could provide better:

- metrics;
- memory-pool accounting;
- cancellation;
- object-store integration;
- repartitioning;
- plan serialization.

But that should be a later refactor.

## Catalog/provider architecture

The existing plain hierarchy also looks like an OOTB engine abstraction rather than an embeddable connector:

```text
IcebergCatalogProvider::try_new()
  ├── list every namespace
  └── construct every SchemaProvider

IcebergSchemaProvider::try_new()
  ├── list every table
  └── construct every TableProvider
```

Those providers are then retained indefinitely in maps.

The cleaner long-term model is:

```text
Long-lived:
  Catalog client / resolver

Query-local:
  Resolved Table
  TableProvider
  physical scan tasks

Execution-local:
  Arrow readers
  writers

Coordinator-only:
  catalog commit
```

Our async provider is moving in that direction.

## Schema mutation is especially concerning

`IcebergSchemaProvider::register_table()` and `deregister_table()` are synchronous DataFusion APIs performing remote async Iceberg operations through:

```rust
tokio::task::spawn_blocking(...)
Handle::current().block_on(...)
futures::executor::block_on(...)
```

That is a strong sign the abstraction boundary is wrong.

Remote catalog DDL should be an explicit async API or factory operation. A synchronous provider-registration method should not be repurposed into remote table creation/deletion.

`ensure_table_is_empty()` also:

- creates an unrelated DataFusion session;
- executes only partition zero;
- discards scan errors through `filter_map(|r| r.ok())`.

That area deserves a separate cleanup.

## Recommended refactor sequence

### 1. Immutable resolved table provider

Make the canonical provider hold a resolved `Table`:

```rust
struct IcebergTableProvider {
    table: Table,
    commit_target: Option<IcebergCommitTarget>,
}
```

Catalog/provider factories create a new one when freshness is required. `scan()` never calls `load_table`.

The compatibility plain provider can remain temporarily.

### 2. Move file planning into `TableProvider::scan()`

Return an `IcebergScanExec` containing partitioned `FileScanTask`s.

This is the most important architectural correction.

### 3. Separate Iceberg reading from planning

Continue using `ArrowReaderBuilder`, but call it only with preplanned tasks inside `execute(partition)`.

### 4. Make commits explicitly coordinator-side

Workers write files and return `DataFile`s. A head-stage node performs:

```text
refresh → validate/reapply → update_table
```

with the request context.

### 5. Replace eager catalog maps

Make async resolution canonical for remote catalogs. Keep the existing eager provider only for compatibility/in-memory usage, potentially deprecating it later.

### 6. Remove synchronous remote DDL

Provide explicit async Iceberg table creation/deletion APIs instead of blocking inside `SchemaProvider`.

## Bottom line

Your instinct is correct:

- `load_table()` in `TableProvider::scan()` is misplaced.
- `plan_files()` inside `ExecutionPlan::execute()` is more seriously misplaced.
- `ArrowReader` itself is reasonable; `TableScan::to_arrow()` collapses too many lifecycle stages.
- The integration should model DataFusion as an embeddable planning/execution library, not simulate an always-live catalog service behind long-lived provider objects.

The target invariant should be:

```text
A DataFusion physical plan contains resolved, immutable work.
Executing that plan performs I/O, not catalog discovery or scan planning.
```
