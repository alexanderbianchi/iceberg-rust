<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Composable Iceberg file reads

Status: design proposal for discussion. The API sketches below are illustrative,
not implemented interfaces or a commitment to their exact names.

Iceberg should expose planned work, file statistics, and reusable read semantics
so engines can use their own decoders and execution machinery. DataFusion is the
first concrete integration target: use its Parquet reader, native filters and
dynamic filters, file scheduling, metrics, and sorted merging while keeping
Iceberg schema and delete semantics in iceberg-rust.

The existing ArrowReader remains a supported convenience API and a consumer of
the extracted components. This proposal does not require every engine to adopt
an Iceberg-owned stream or convert query expressions into Iceberg expressions.

Implementation observations refer to the [inspected iceberg-rust reader][reader]
at `e9d2a8a`, DataFusion 55.x, and [DFD #801][dfd-801] at `54187c2`.
The fork branch containing this document may have an older code baseline; links
pin the implementation being discussed.

## Requirements and desired outcomes

1. **Native predicates.** Engine query predicates and live dynamic filters reach
   the engine's decoder without an engine-to-Iceberg-to-engine conversion chain.
   A separate conversion for manifest pruning may still be useful.
2. **Native options.** Preserve the configured decoder's options, storage hooks,
   caching, and runtime choices. Do not mirror all Parquet settings in Iceberg.
3. **Independent planning.** Accept tasks from Rust Iceberg planning or an
   external planner, including Iceberg Java through Comet. Execution must not
   require loading a catalog or replanning a table. Supplied work must include
   the read schema, name mapping, partition/spec and sort definitions, and
   encryption metadata needed to interpret and open the file.
4. **Per-file reads.** Construct an individual read so the engine can choose
   assignment, concurrency, concatenation, or sorted merging. Constructing a
   stream handle need not perform file I/O.
5. **Shared Iceberg semantics.** Reuse field-ID resolution, schema evolution,
   defaults, partition constants, metadata columns, and delete handling.
6. **Deletes inside decoding.** Prefer position selections and equality-delete
   predicates applied by Parquet. Deletes are mandatory even when optional query
   filter pushdown is disabled.
7. **Statistics before execution.** Expose selected-file metrics during planning,
   without a second manifest pass or a Parquet footer read. Preserve per-file
   information when grouping, distributing, and serializing work.
8. **Engine integration.** In datafusion-iceberg, implement the existing
   `FileSource`/`FileScanConfig` contracts, including expression traversal,
   pushdowns, metrics, and native execution scheduling.
9. **Explicit capabilities.** Unsupported optimizations retain a correct
   residual or fallback. Unsupported required semantics fail rather than
   silently returning incorrect rows.
10. **Composability.** Eager and lazy discovery can feed the same per-file read
    machinery. Engines may use the complete Arrow reader or individual semantic
    components. Do not introduce an arbitrary planned-file-count limit.

The desired end states are:

- Existing applications continue to use `TableScan::to_arrow()`/ArrowReader.
- Engines can open one planned task and own the cross-file execution strategy.
- DataFusion can use native Parquet decoding with live native predicates and
  Iceberg-prepared delete predicates in the same decoder.
- With eager discovery, optimizers and distributed planners can consume
  statistics for the actual selected files and assigned subsets before execution
  starts. Lazy discovery exposes file metrics incrementally and uses snapshot
  estimates or unknowns until the selected set is known.

## Responsibilities and data flow

```text
Rust planner or external planner
    |
    +--> executable task + selected-file metadata
            |                        |
            |                        +--> file / group / scan statistics
            v
    Iceberg read requirements
    IDs, schema rules, hidden fields, deletes, row identity
            |
    physical metadata loaded once by the selected decoder
            |
    Iceberg binding + shared delete preparation
            |
            +--> required projection and position selections
            +--> mandatory equality-delete predicates
            |
            v
    native decoder <---------------- engine query / dynamic filters
            |
    remaining Iceberg batch transformations
            |
    engine scheduling / concatenation / sorted merge
```

The engine decides how work executes. Iceberg decides what reading a selected
file means. Preparing delete data may require asynchronous I/O; constructing a
predicate from already-prepared state is synchronous. Whole-file runtime pruning
should happen before unnecessary footer or delete I/O where metadata permits it.

## Relationship to the Java File Format API

The [Java File Format API][java-blog] provides a useful decoder boundary.
`FormatModel` and its registry select a format/output representation. Its
[`ReadBuilder`][java-read-builder] accepts a file, byte split, projected schema,
static Iceberg expression, field-ID constants, name mapping, and batch size.
Filtering may be incomplete; the caller remains responsible for residuals.

This is a Java implementation API, not a language-neutral wire protocol. It does
not itself provide live engine predicates, task assignment, selected-file
statistics, or sorted merging. In [Spark's reader][java-row-reader], Iceberg
delete processing surrounds the format reader and widens its required schema.

For Rust, preserve this separation without copying the registry hierarchy:

- Use concrete immutable semantic descriptions and borrowed accessors.
- Put traits at replaceable behavior boundaries, not on every metadata type.
- Let backend-native predicates and options retain their own types.
- Make storage, planning, decoder choice, and optimization support independent.
- Keep DataFusion types out of iceberg-rust; use the existing Arrow/Parquet
  contracts where the implementation is specifically Arrow/Parquet based.

Optional execution strategies do not make delete or schema correctness optional.

## Proposed Iceberg types and interfaces

### Planning results and statistics

The planner should expose selected `DataFile` metadata before it is reduced to a
`FileScanTask`. A conceptual result is:

```rust
pub struct PlannedFile { /* task and selected metadata; private fields */ }

impl PlannedFile {
    pub fn task(&self) -> &FileScanTask;
    pub fn data_file(&self) -> &DataFile;
}
```

Consumers can aggregate while iterating or retain only the metrics they need.
The implementation must choose an ownership/streaming model that avoids retaining
entire manifests merely to keep one selected entry alive. This need not enlarge
the existing task wire format: an opt-in richer planning result or companion
metadata path can coexist with the current task stream.

Use Iceberg field IDs and values in this API. DataFusion `Statistics`, scalar
conversion, and projection-index mapping belong in datafusion-iceberg.

Snapshot estimates/sketches and manifest-list partition summaries remain separate,
optional planning-context inputs with snapshot, schema, and partition-spec
provenance. They cannot be reconstructed from selected `DataFile` metrics or
silently relabeled as selected-subset statistics.

### Read preparation

```rust
pub struct ReadRequirements { /* requested and hidden fields, semantic rules */ }
pub struct PhysicalFileMetadata { /* physical schema, IDs, row layout */ }
pub struct BoundFileRead { /* resolved projection, defaults, row identity */ }
pub struct PreparedDeletes { /* shared position/equality delete state */ }
pub struct ScanReadContext { /* local I/O/runtime and scan-scoped caches */ }
```

These names describe responsibilities, not a requirement to add five new public
types. Prefer extracting existing implementations and merging descriptions where
they share a lifetime. Required operations are:

1. Derive requirements from the task, including equality-delete keys absent from
   the requested output and any required original row positions.
2. Bind requirements to metadata already loaded by the decoder. Share the same
   ID/default/type decisions with projection and predicate adaptation.
3. Load applicable deletes using shared scan-scoped state.
4. Produce decoder selections/predicates and remaining batch transformations.

Binding must precede native predicate evaluation. Applying an initial default
only after a predicate treated the missing column as null can lose valid rows.
Physical metadata means more than an Arrow schema with field names.

Keep I/O handles, runtimes, caches, and live filters local. Portable tasks/settings
are separate from execution state; defaults such as concurrency should be
resolved deliberately on the execution host. Share delete preparation across
partitions of one scan. Cross-snapshot/schema cache reuse needs stronger keys
than file paths alone.

### Optional decoder traits for the Arrow convenience path

```rust
pub trait FileDecoder: Send + Sync {
    type Predicate: Send + Sync;
    type File: OpenedFile<Predicate = Self::Predicate>;

    fn open(
        &self,
        input: FileInput,
    ) -> impl Future<Output = Result<Self::File>> + Send;
}

pub trait OpenedFile: Send {
    type Predicate: Send + Sync;

    fn metadata(&self) -> &PhysicalFileMetadata;
    fn read(
        self,
        request: DecodeRequest<Self::Predicate>,
    ) -> Result<DecodedRead<Self::Predicate>>;
}

pub struct DecodeRequest<P> {
    pub requirements: BoundFileRead,
    pub deletes: PreparedDeletes,
    pub predicate: Option<P>,
}

pub struct DecodedRead<P> {
    pub batches: DecodedBatchStream,
    pub residual: Option<P>,
}

pub struct DecodedBatch {
    pub batch: RecordBatch,
    pub original_positions: Option<RowPositions>,
}
```

`open` loads physical metadata once and retains the reader. `read` constructs lazy
decoding using the bound requirements. In this trait sketch, every emitted row
must already satisfy the requested deletes: any fallback is part of the returned
read and precedes an output limit. A backend unable to enforce deletes must error;
the caller does not infer their status from the query residual. Backend options
live on the configured decoder, rather than in a universal options struct. An
unsupported query filter remains a residual that the caller must evaluate. A bare
convenience API must enforce that residual or reject the unsupported configuration,
never hide it.

This is an Arrow-batch interface. It is optional for engines with richer native
execution objects: DataFusion can consume the preparation APIs directly and
retain its morsel execution. A generic associated batch type alone would not
make Iceberg's Arrow evaluators work for non-Arrow engines.

Original positions are optional only when requirements permit. They mean file
ordinals before filters, page/row-group skips, or split processing, not offsets
within returned batches. Position deletes may eliminate the need for a later
position column, but requested metadata/lineage columns can still require it.

## Mapping the current ArrowReader

| Existing component | Reuse / extraction | New integration work |
| --- | --- | --- |
| `ArrowReader::read`, private `FileScanTaskReader::process` | Expose one-task lazy reads; retain the multi-task convenience wrapper | Let the engine own grouping, concurrency, buffering, and merging |
| `DeleteFileIndex` and scan task construction | Keep sequence, partition/spec, path, and DV applicability in Iceberg planning | External planners must supply equivalent task semantics |
| `BasicDeleteFileLoader`, `CachingDeleteFileLoader`, `DeleteFilter` | Reuse file loading, delete-file schema evolution, and shared preparation | Separate execution lifetime and prepared delete state from query predicates |
| `build_deletes_row_selection` | Reuse positional/DV selection logic | Compose with native decoder selections in original file coordinates |
| `PredicateConverter`, `get_row_filter` | Reuse equality-delete Arrow evaluation | Expose a prepared predicate factory without combining engine query expressions |
| `RecordBatchTransformer` | Reuse synchronous per-batch evolution, promotion, reordering, constants, and defaults | Expose shared binding before native predicate/pruning evaluation |
| Projection/name-mapping helpers and `configure_arrow_reader_metadata` | Extract existing physical ID resolution | Adapt the resulting mapping to engine projections and expressions |
| Row-lineage helpers | Reuse supported metadata synthesis | Preserve original positions through native filtering/splitting |

The [batch transformer][transformer] already has no stream dependency: ArrowReader
maps it over decoded batches. Equality-delete application is more coupled:
today it creates an Iceberg predicate, combines it with the task predicate, and
builds a Parquet row filter. Extract that boundary instead of rewriting all delete
semantics or requiring DataFusion query predicates to take the same route.

Retain the current reader as the first consumer of extracted components and keep
its tests passing. Existing limitations are not automatically solved by exposing
the machinery; for example, arbitrary complex initial defaults and every
metadata column are not promised by this proposal.

## Equality deletes inside the Parquet scan

Use the existing [`ArrowPredicate`][arrow-predicate] contract: a projection of
required columns plus evaluation returning a Boolean keep mask. Parquet's
[`RowFilter`][row-filter] composes these predicates, refines row selections, and
then decodes the final output projection.

```text
DataFusion query / dynamic expressions --> native ArrowPredicates --+
                                                                  |
Iceberg prepared equality deletes -------> delete ArrowPredicate --+--> RowFilter
                                                                         |
                                                              surviving output rows
```

The preferred Iceberg API constructs fresh predicate instances from shared
prepared deletes and a physical binding. Reuse existing delete evaluation first;
changing its internal representation to a hash matcher is a separate decision.
Equality keys must use Iceberg IDs, compatible types/defaults, and Iceberg null
matching. Do not substitute ordinary SQL `NOT IN` semantics.

### Compose an existing Parquet source first

DataFusion 55.x builds its native row filter from physical expressions. First
prototype an Iceberg `PhysicalExpr` matcher over shared prepared deletes and
configure the existing `ParquetSource` with the combined delete and supported
query predicates:

```rust
// Existing DataFusion APIs; constructing and binding the predicate is prototype work.
parquet_source
    .with_predicate(combined_predicate)
    .with_pushdown_filters(true)
```

`with_predicate` replaces the source predicate, so composition must retain any
existing query and live dynamic predicates. Async delete loading precedes
synchronous expression evaluation. The matcher must expose its required columns
and preserve prepared state through expression rewriting and decoder rebuilding.

Prove two properties before relying on this approach:

1. **Per-file binding.** A source normally serves multiple files, whose applicable
   deletes and physical schemas can differ. Specialize the delegated read using
   existing file/morsel interfaces, preserving native scheduling and shared delete
   preparation. A scan-wide matcher must not apply one file's deletes to another.
2. **Mandatory enforcement.** Enabling pushdown alone is insufficient. The current
   row-filter construction can omit unsupported conjuncts or ignore construction
   errors. Verify that the delete expression is evaluated, or retain a correct
   fallback before any output limit. Disabling optional query-filter pushdown
   must not disable deletes; query residuals must still be enforced separately.

A new DataFusion hook is a fallback, not a prerequisite. Introduce one only if
this prototype demonstrates that existing interfaces cannot safely express
per-file binding or guaranteed enforcement. Such a hook would need to compose
required predicates with native query predicates, propagate failures, and survive
decoder/morsel rebuilding. Sharing prepared delete data must not imply sharing a
mutable predicate instance between decoders. Post-decode matching remains a
correctness fallback, not the preferred default integration.

## DataFusion integration

In datafusion-iceberg, implement existing engine interfaces:

```rust
struct IcebergFileSource {
    inner: Arc<dyn FileSource>, // Configured native Parquet source.
    scan: Arc<ScanReadContext>,
}

impl FileSource for IcebergFileSource { /* delegate and adapt semantics */ }
impl PhysicalExprAdapterFactory for IcebergExprAdapter { /* schema binding */ }
impl ParquetFileReaderFactory for IcebergReaderFactory { /* optional FileIO bridge */ }
```

The [FileSource contract][file-source] provides filter/projection/sort pushdown and
expression traversal. Re-wrap updated sources returned by pushdown methods so
the Iceberg layer remains present. Keep live expressions visible through
`apply_expressions`. Preserve required delete fields until matching is complete,
and prevent decoder limits from counting rows that later delete processing removes.

Native Parquet in the inspected DataFusion version uses `create_morselizer`.
Preserve this [morsel API][morsel] execution path rather than forcing it through
the optional stream decoder trait above. Morsel APIs are experimental; their
types should not become core iceberg-rust dependencies.

`PhysicalExprAdapterFactory` is a useful binding point but currently receives
schemas, not arbitrary task context. Propagating file-specific constants and
bindings needs prototyping. `ParquetFileReaderFactory` alone only replaces byte
access; it does not provide semantic or predicate composition hooks.

DFD #801 demonstrates task-to-`PartitionedFile` integration and routing with the
current Iceberg reader. It does not yet demonstrate native Parquet decoding or
dynamic pushdown. Its typed task extension needs explicit codec handling;
extensions are not automatically a distributed wire format. Preserve original
URIs and actual storage context when moving beyond its placeholder object store.

## Dynamic filtering and ordering

Dynamic filters remain native engine expressions. Schema adaptation may rewrite
column references while retaining shared update state; taking one static
`current()` snapshot loses subsequent updates. DataFusion already has generation
tracking and cached remapping. Conversion/binding for optional Iceberg-level file
pruning should be cached per coherent predicate generation and schema context.

Support progressively later opportunities: selected-file pruning before opening,
row-group/page pruning while reading, and decoder row filtering. A task-start
callback alone cannot update an already-open file. Unknown metrics retain files;
unsupported translations retain residuals. Advisory runtime restrictions must
remain safe as they evolve, and failures must not remove planned constraints or
mandatory deletes. Eager discovery is compatible with all of these read-time
optimizations, but cannot recover already-spent manifest I/O.

Expose per-file sort metadata and validated ordering information to the engine.
Sorted files with overlapping ranges cannot simply be concatenated into an
ordered partition. The engine chooses nonoverlapping groups or a sorted merge,
controls fan-in and memory, and advertises only ordering preserved by projection,
schema evolution, filtering, and scheduling. Check transform, null, and value
ordering compatibility; an Iceberg sort-order ID alone is not a DataFusion
ordering proof. A merge fan-in policy does not require a cap on file planning.

## Statistics contract

DFD #801 copies whole-scan statistics into `FileScanConfig`, but file entries have
no individual statistics and worker subsets deliberately become unknown. Its
inherited [selected-file statistics][selected-stats] reread manifests because
`FileScanTask` drops metrics. The richer planning result should remove this second
pass and support file -> assigned group -> scan aggregation.

| Metadata | Required interpretation |
| --- | --- |
| Record count | Whole-file physical rows; output rows may change through deletes, filters, or limits |
| Lower/upper bounds | Conservative bounds, possibly truncated; not automatically exact extrema |
| Null/value/NaN counts | Field-ID metrics; absent means unknown, and leaf counts do not imply parent counts |
| Column sizes | Compressed physical column bytes, distinct from full-file I/O and decoded Arrow memory |
| Snapshot NDV/sketches | Snapshot-scoped estimates; never sum NDVs or present them as exact selected-file counts |
| Split tasks | Whole-file counts and sizes do not become exact split metrics; avoid double counting |
| Partition summaries | Preserve manifest-list scope and partition-spec semantics; do not confuse summaries with selected-file column metrics |

Retain raw metrics and provenance separately from engine estimates. Missing or
incomplete metrics must remain unknown unless schema/default rules justify an
inference. Deletions preserve conservative bounds but may change exact counts and
extrema. Filtering does not turn original-file counts into observed output counts.

Eager discovery exposes complete selected-file aggregates for join planning,
parallelism, and byte balancing. Lazy discovery can expose per-file metrics as
they arrive, including for runtime pruning, but cannot supply a complete selected
aggregate before discovery finishes. Preserve metadata or correctly derived file
statistics across serialization and recompute subset aggregates after assignment.
Keep snapshot-only estimates separately identified. Do not add DataFusion
statistics types to iceberg-rust or require decoding to obtain metrics already
present in manifests.

## Delivery and validation

1. Expose selected-file metadata and extract one-task reads without changing the
   existing reader's semantics. Keep reader configuration separate from planning.
2. Extract shared schema binding and delete preparation; keep ArrowReader using
   them. Introduce only the traits demonstrated necessary by actual consumers.
3. Prototype a configured native DataFusion Parquet source with an Iceberg delete
   `PhysicalExpr` and shared schema adaptation. Prove per-file binding and mandatory
   enforcement using existing interfaces before proposing a new DataFusion hook.
4. Add dynamic pruning, validated ordering, and per-file/subset statistics using
   the same task and metadata representation.

Validate existing and native paths against the same fixtures: renamed/reordered
and missing columns, promotions/defaults, equality keys absent from output,
null-valued delete keys, positional deletes/DVs combined with ranges and pruning,
row lineage, live filter updates after opening, LIMIT after deletes, sorted files
with overlapping ranges, and codec round-trips including statistics precision.
Exercise cache sharing, cancellation, errors, and decoder reconstruction. Disabling
query-filter pushdown must never disable deletes. Compare results and plans; make
performance claims only after measuring representative workloads.

Open decisions are whether existing DataFusion interfaces suffice for mandatory
per-file predicates, how bindings reach its expression adapter, ownership of
selected metadata, and which parts merit a stable public API. No global format registry, generic non-Arrow
evaluator framework, or engine execution-plan abstraction is required initially.

## Related work

- [iceberg-rust #3514][reader-options]: reader settings independent of planning;
  review feedback supports Iceberg-owned options applied by the reader builder.
- [datafusion-iceberg #67][dfi-67]: eager planning and retained per-partition work.
- [datafusion-iceberg #55][dfi-55]: ordered reads and engine-side merging.
- [iceberg-rust #2671][eager-discussion]: eager-planning discussion, shared delete
  loading, and integration ownership.
- [iceberg-rust #2376][dynamic-start] and [#3343][dynamic-epic]: task-start and
  broader dynamic filtering. Their desired capabilities are not all shipped.
- [datafusion-comet #6737][comet]: externally planned tasks and native filtering.
- [DFD #773][dfd-773], [#787][dfd-787], [#788][dfd-788], [#789][dfd-789], and
  [#801][dfd-801]: eager planning, scan sizing, assignment, selected statistics,
  and standard file-scan integration. [#795][dfd-795] tracks statistics follow-up.
- [Sail's Iceberg scan][sail-scan] demonstrates native DataFusion Parquet reads;
  its [delete path][sail-deletes] illustrates the restrictions introduced when
  original positions are reconstructed by counting returned rows.

[reader]: https://github.com/apache/iceberg-rust/blob/e9d2a8a99399ab0440402c5935e37f797cbae60d/crates/iceberg/src/arrow/reader/pipeline.rs
[transformer]: https://github.com/apache/iceberg-rust/blob/e9d2a8a99399ab0440402c5935e37f797cbae60d/crates/iceberg/src/arrow/record_batch_transformer.rs#L694
[java-blog]: https://iceberg.apache.org/blog/apache-iceberg-file-format-api/
[java-read-builder]: https://github.com/apache/iceberg/blob/2856631da4d81e79ece85c3694bfd81a86ca612e/core/src/main/java/org/apache/iceberg/formats/ReadBuilder.java
[java-row-reader]: https://github.com/apache/iceberg/blob/2856631da4d81e79ece85c3694bfd81a86ca612e/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java
[arrow-predicate]: https://docs.rs/parquet/59.3.0/parquet/arrow/arrow_reader/trait.ArrowPredicate.html
[row-filter]: https://docs.rs/parquet/59.3.0/parquet/arrow/arrow_reader/struct.RowFilter.html
[file-source]: https://docs.rs/datafusion/55.0.0/datafusion/datasource/physical_plan/trait.FileSource.html
[morsel]: https://docs.rs/datafusion-datasource/55.0.0/datafusion_datasource/morsel/index.html
[selected-stats]: https://github.com/datafusion-contrib/datafusion-distributed/blob/54187c2212eb4e50bc07991526edf9bd9e48224b/iceberg/src/column_statistics.rs#L18
[reader-options]: https://github.com/apache/iceberg-rust/pull/3514#pullrequestreview-5626952689
[dfi-67]: https://github.com/apache/datafusion-iceberg/pull/67
[dfi-55]: https://github.com/apache/datafusion-iceberg/pull/55
[eager-discussion]: https://github.com/apache/iceberg-rust/pull/2671
[dynamic-start]: https://github.com/apache/iceberg-rust/issues/2376
[dynamic-epic]: https://github.com/apache/iceberg-rust/issues/3343
[comet]: https://github.com/apache/datafusion-comet/issues/6737
[dfd-773]: https://github.com/datafusion-contrib/datafusion-distributed/pull/773
[dfd-787]: https://github.com/datafusion-contrib/datafusion-distributed/pull/787
[dfd-788]: https://github.com/datafusion-contrib/datafusion-distributed/pull/788
[dfd-789]: https://github.com/datafusion-contrib/datafusion-distributed/pull/789
[dfd-801]: https://github.com/datafusion-contrib/datafusion-distributed/pull/801
[dfd-795]: https://github.com/datafusion-contrib/datafusion-distributed/issues/795
[sail-scan]: https://github.com/lakehq/sail/blob/a58afdd6fc2565702460ef2e48a1e5c16cd4d7e1/crates/sail-iceberg/src/datasource/scan.rs#L1136
[sail-deletes]: https://github.com/lakehq/sail/blob/a58afdd6fc2565702460ef2e48a1e5c16cd4d7e1/crates/sail-iceberg/src/physical_plan/delete_apply_exec.rs#L95
