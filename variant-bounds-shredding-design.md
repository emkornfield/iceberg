# Shredding Variant Bounds in V4 Manifests

**Status:** prototype passing · leaf-level only · no aggregation · **blocked on `content_stats` write (#13694)**

Can a variant column's statistics bounds be shredded the way data files shred variant
columns — and does the read side actually get faster? This note captures the mechanism, a
working prototype, and an honest look at the payoff.

> **Verdict.** Shredding the bounds *as a variant column* (name-addressed, with a residual
> fallback) is mechanically sound and reuses the entire existing shredding stack — the write
> side is a natural extension. The **read-side payoff at the leaf level is modest**: it reduces
> bytes and decode cost for wide bounds objects, but it does *not* skip work algorithmically.
> The compelling win — pruning whole files or subtrees — lives at the aggregation level, which
> is deliberately out of scope here.

---

## 1. How bounds are tracked today

For a `variant` column, `content_stats.<field>.lower_bound` / `upper_bound` hold a **variant
object keyed by normalized JSON path**, produced from the file's already-shredded physical
columns. The value type is the variant type itself — stored as an unshredded blob.

| Stage | Where | What happens |
|---|---|---|
| **Produce** | `ParquetMetrics.variant()` ~L405 | Walks the shredded Parquet schema, pulls each shredded leaf's column statistics, packs them into a `ShreddedObject` keyed by `PathUtil.toNormalizedPath(...)`, wraps as `Variant.of(metadata, bounds)`. |
| **Store** | `FieldStatsStruct` / `StatsUtil` | The bound field is declared as the variant type and serialized as one binary blob (metadata + value). One fixed `content_stats` type per manifest. |
| **Read** | `InclusiveStatsEvaluator` ~L141 | `statsFor(fieldId).lowerBound().value().asObject().get(bound.path())` — deserialize the whole blob, look up by normalized path. |

## 2. Why it's unshredded today

The spec mandates it (`spec.md:889`: "both bounds are unshredded `variant`"). The structural
reason is `spec.md:745`: the `content_stats` type is derived from table metadata and is
**uniform across every file entry in a manifest**, like the `partition` struct. Two objections
follow, and both are specific to one framing:

- **No field IDs.** Promoting variant subfields to typed, ID-addressed `content_stats`
  sub-structs needs stable IDs; paths are schema-on-read strings.
- **No uniformity.** Each file infers its own shredded layout, so the key set and types vary
  per file — a single fixed struct type can't carry a per-file layout.

## 3. The idea: shred the bounds as a variant column

Both objections dissolve if we don't *promote paths to typed structs* but instead keep the
bound a **variant column and shred that column** — exactly what a data file does. Variant
shredding is name-addressed (the `metadata`/`value`/`typed_value` groups get no field IDs), and
it always has a residual: uniform, frequent paths get typed columns; everything else falls to
the untyped `value`. So no global uniformity and no IDs are required — only per-manifest-node
local structure, which is symmetric to per-data-file structure.

```
lower_bounds                         variant column (a manifest bound field)
├── metadata
├── value                            ← object-level residual (unshredded paths live here)
└── typed_value                      the shredded object
    ├── $['event_id'] → value + typed_value
    └── $['name']     → value + typed_value
```

Every shredded field carries **both** a `value` and a `typed_value`. That per-field residual is
the crux of the read-side correctness rule below.

## 4. Write side — one schema, chosen by intersection

Gate on Parquet, buffer the manifest entries, analyze the per-file bound objects with the
existing `VariantShreddingAnalyzer`, and feed the result through `variantShreddingFunc` — the
same hook data writes use. Lower and upper bounds must share **one** schema, and it must be
their **intersection**, not their union:

```java
// shred a path iff BOTH bounds admit it at the same leaf type, so every
// shredded path has a matched min/max column and stays spec-conformant
Type shreddedLower = analyzer.analyzeAndCreateSchema(lowerObjects, 0);
Type shreddedUpper = analyzer.analyzeAndCreateSchema(upperObjects, 0);
Type shared = intersectShredSchemas(shreddedLower, shreddedUpper);

appender = InternalData.write(FileFormat.PARQUET, file)
    .schema(manifestSchema)
    .variantShreddingFunc((fid, name) -> sharedByStatsFieldId.get(fid)) // NEW
    .build();
```

A path shredded in only one bound gives the evaluator half a range — useless for pruning and a
half-empty column. Intersection also enforces the spec rule that lower and upper share a variant
type. In practice the two analyses usually coincide (`ParquetMetrics` records a path only when
both bounds exist), so intersection matters for the edge cases: truncation shifting a type on
one side, or width-widening diverging between the bounds.

## 5. Read side — never reconstruct from a subset

Because each leaf shreds independently, the reader must decide per manifest file, against that
file's actual Parquet schema, whether a queried path is a typed column or lives in the residual.

> **Invariant.** Projection may only ever cause you to read *more* than the minimal blob — never
> reconstruct a variant subtree from a strict subset of its own columns. For a shredded path,
> project **both** `value` and `typed_value` plus `metadata`; for a residual path, project the
> object-level `value`. Skipping the per-field `value` yields a *too-tight* bound.

The backstop is monotonic: `InclusiveStatsEvaluator` treats a missing bound as
*rows-might-match*. So at the leaf level, a fumbled projection degrades to "didn't prune" (safe),
never to "dropped a matching file." The only way to get a wrong answer is presenting a
present-but-incomplete bound — which the invariant forbids.

## 6. Does the read side actually pay off?

This is the honest part, and the answer is: **at the leaf level, less than you'd hope.**

- **You still read every entry.** Planning iterates all manifest entries regardless. Shredding
  reduces bytes-and-decode *per entry* (project one path instead of decoding the whole blob) — a
  constant factor, not algorithmic skipping.
- **The blob is already small.** Bounds are truncated summaries. Decoding one small variant
  object per entry is cheap; projecting one field out of it saves little CPU on narrow objects.
- **The residual is all-or-nothing.** If any queried path landed in the residual — likely, given
  heterogeneous leaves — you read the whole residual `value` and the projection win evaporates
  for that query.
- **Row-group skipping rarely fires.** Parquet could skip manifest row-groups using a shredded
  column's own min/max, but only if entries are clustered by that variant subfield. Manifests are
  ordered by sequence/partition, not by an arbitrary variant path, so the stats won't prune.
- **Costs are real.** Per-manifest analysis, a wider and per-file-varying manifest schema, and 4
  columns per shredded path (value+typed × lower+upper) plus footer stats. For small or sparse
  manifests the metadata overhead can exceed the savings.

**Where it does pay off:** wide bounds objects + selective predicates + large manifests, where
per-entry I/O dominates — and, decisively, **aggregation**: rolled-up typed bounds at parent
nodes let a query skip a whole subtree without reading its entries. That is the algorithmic win,
and it needs the parent-level merge we deferred. Leaf-level shredding is best seen as the
*substrate* that makes that aggregation possible, not a standalone read optimization.

### 6a. The one leaf-level lever worth building: skip the residual blob

There *is* a real leaf-level I/O win, but it requires read-path work that does not exist today.
A shredded field's per-row value lives only in its own `value`/`typed_value` — never in the
object-level residual `value`, which holds the entire cold long-tail as one blob. So if a query
touches **only shredded paths**, the reader can project `{metadata, P.value, P.typed_value}` and
**drop the object-level `value` and every sibling field** — skipping the cold blob entirely.
That is data-skipping, not just cheaper decode. (If any queried path is residual, the object
`value` must be read and the win is lost — all-or-nothing per query, but the all-shredded case is
common for predicates on hot fields.)

What's missing today (inspection is free; acting on it is not):

- `PruneColumns.variant()` is a no-op (`return variant;`), so `project(schema)` always requests
  the whole variant column — Parquet reads the residual blob regardless.
- `VariantReaderBuilder.object()` builds readers for *all* `typed_value` fields.
- Variant is opaque in the Iceberg schema, so predicate paths must reach the reader as a
  side-channel (the natural extension of #17433 `projectStats(...)` down to sub-paths).

Enabling it needs: a projection hint carrying `BoundExtract` paths; a variant-aware
`PruneColumns` that prunes `typed_value` to requested fields and drops the object `value` when no
requested path is residual; and a partial-object reader that reconstructs from the projected
fields and tolerates an absent object `value` (presence then comes from `metadata` and the
projected fields' definition levels). Note this does **not** violate the §5 invariant: dropping
the object residual is fine when every queried path is shredded, because each shredded field's
own subtree is still read completely.

## 7. Prototype status

A runnable prototype (`parquet/src/test/java/org/apache/iceberg/parquet/TestVariantBoundsShredding.java`)
shreds a stand-in manifest's bound column with the real analyzer/writer and reads paths back — no
new production code. Every storage path a bound field can take is exercised:

| Field profile | Decision | Stored in | Read source |
|---|---|---|---|
| uniform int64 / string, frequent | shredded | typed column | `typed_value` |
| widened int family, one narrower row | shredded | typed + per-field residual | `value` + `typed_value` |
| mixed type (int64 & string) | not admitted | object residual | `value` |
| infrequent (< 10%) | pruned | object residual | `value` |
| absent in a file | — | — | `null` → might-match |
| present but null-valued | residual | object residual | NULL-typed variant |
| type-divergent across lower/upper | demoted | object residual (both) | `value` |

The narrower-row test proves the point empirically: the int32 row is provably absent from the
typed column (`numNulls = 1`, typed-only min excludes it), yet full `{value, typed_value}`
reconstruction still returns its true bound. **Next:** an end-to-end write→read→prune cycle
writing lower *and* upper against the intersected schema.

## 8. Upstream landscape

- **#13694** *Introduce content_stats* — the umbrella draft that would populate `content_stats`
  on write. Long-stale; everything below was split out of it.
- **#17433** *Read content stats from v4 Manifest* — active; adds `projectStats(...)`, the
  natural home for path-level projection.
- **#17413** *content stats evaluator* — merged; the variant read path used above.
- **#16025** *V4 Adaptive Metadata Tree spec* — the no-manifest-list tree; where aggregation
  would live.

No live PR wires `content_stats` *writing* into the v4 manifest writer — it's `null` today. Any
bounds-shredding work sits on top of that missing step.

## 9. Recommendation

Treat leaf-level bounds shredding as an **enabler for aggregation**, not a leaf-read
optimization. Land the write-side substrate (it's cheap and reuses existing code), keep the read
side correct-and-simple behind the projection invariant, and reserve the engineering budget for
the parent-node merge that turns shredded typed bounds into subtree pruning — the only place the
read side gets an algorithmic win. Until then, the unshredded blob is a perfectly reasonable
default at the leaf.

## 10. Read-path implementation design (sub-variant projection)

The read machinery is closer to supporting this than a first pass suggests. The gap is upstream
of the reader, not in it.

### What already works — the reader is not the gap

- `VariantReaderBuilder.object()` builds field readers from the fields present **in the
  `typed_value` group it is handed** (`VariantReaderBuilder.java:152`), and already handles a
  null object `value` reader via a sentinel definition level (`:148`).
- `ShreddedObjectReader` reconstructs a **partial** object: with `valueReader == null` it uses
  the first field's column for object presence (`ParquetVariantReaders.java:307`), builds an
  empty object when the residual is `MISSING` (`:318`), and adds only the field readers it was
  given. `read(metadata, null, dl)` returns `MISSING` (`:465`), so dropping the residual is safe.
- Definition levels are per-path: pruning sibling fields or the object `value` does not change a
  kept field's max definition level, so kept readers stay correct.

**Conclusion:** handed a *pre-pruned* variant group (`typed_value` with only the requested
fields; object `value` dropped when safe), the existing reader reconstructs exactly the partial
object we want. No reader change is required.

### The four changes required

1. **Carry requested sub-paths into the read (new input).** Variant is opaque in the Iceberg
   schema, so predicate paths need a side-channel: a variant projection hint,
   `Map<Integer fieldId, Set<String normalizedPath>>`, on `Parquet.ReadBuilder` / `ReadConf`.
   For manifest reads, `V4ManifestReader` derives it from the scan filter's `BoundExtract` paths
   — the extension of #17433 `projectStats(...)` below field granularity.

2. **Make `PruneColumns.variant()` prune** (today `return variant;`, `PruneColumns.java:157`).
   Given the file's `variantGroup` and the requested paths for that field id, emit a pruned group:
   keep `metadata` always; descend `typed_value` and keep only the requested shredded fields,
   **each with both its `value` and `typed_value`** (never drop a kept field's residual channel —
   the §5 invariant); keep the object-level `value` iff some requested path is *not* shredded in
   this file (needs the residual) or nothing is projected, else drop it. Nested paths
   (`$['a']['b']`) require recursive descent. The visitor must be threaded the path set.

3. **Build the variant reader consistently with the pruned group (the one real knot).** `ReadConf`
   builds the reader model from `typeWithIds` (full) while `projection` is pruned
   (`ReadConf.java:~121`). If the variant is pruned only in `projection`, the reader — walking the
   full group — would construct readers for columns Parquet never read. So the variant reader must
   be built over the **pruned** variant subtree (column descriptors/DLs still resolve from the
   physical file; they are identical for kept paths). This is the only non-trivial plumbing change.

4. **Evaluator: no change.** `InclusiveStatsEvaluator.extractLowerBound` still does
   `asObject().get(bound.path())`; the partial object contains the requested paths, and anything
   absent reads as `null` → rows-might-match (safe). It only asks for paths it projected.

### The payoff condition

The object `value` (cold blob) is dropped only when **every requested path is shredded** in this
manifest — then Parquet skips the blob and all sibling field columns. If any requested path is
residual, the blob is kept (it holds that path plus the cold tail) and there is no skip for that
query. All-or-nothing per query; the all-shredded case is the common analytical one.

### Prototype path (no generic-API change)

The manifest reader is a controlled caller, so the capability can be demonstrated without
touching `Parquet.ReadBuilder`: hand-build a pruned requested `MessageType` (metadata +
`$['event_id']`'s `{value, typed_value}`, dropping `lower_bounds.value` and siblings), open
`ParquetFileReader` with that requested schema, and build the reader with a `VariantReaderBuilder`
over the pruned group. Assert: (a) `event_id` reconstructs correctly, (b) the residual column
chunk is never read, (c) a residual-path query keeps the blob.

### Status: landed vs remaining

**Landed (production):** `ParquetSchemaUtil.pruneVariantPaths(schema, Map<fieldId, Set<path>>)`
implements change #2 — the variant-aware pruning — as a pure, reusable utility (keeps `metadata`,
keeps only requested shredded fields with both channels, drops the object `value` unless a
requested path is residual). Additive; no existing read path calls it yet, so behavior is
unchanged for current readers.

**Proven (tests, `TestVariantBoundsShredding`):**

- *Byte-level I/O reduction.* `prunedProjectionSkipsResidualColumnChunks` sets the pruned schema
  as the Parquet requested schema — exactly what `ReadConf.reader()` does via
  `setRequestedSchema` — and confirms the row group loads `event_id`'s chunk while the residual
  `value` and the `name` sibling chunks are never read (`getPageReader` throws).
- *Partial reconstruction.* `readerReconstructsPartialObjectFromPrunedGroup` reads through the
  Iceberg reader built over the pruned group and gets an object with only `event_id`
  (`numFields == 1`), value correct — with no reader-code change. This also settles the change-#3
  concern empirically: the reader reconstructs partial objects correctly, so the ReadConf knot is
  purely about *aligning the physical projection*, not reader correctness.
- *Residual fallback.* `prunedProjectionKeepsOnlyRequestedPathAndDropsResidual` shows a request
  for a residual path keeps the object `value`.

**Landed (read plumbing, gated):** the projection hint is now threaded end-to-end.
`Parquet.ReadBuilder.withVariantProjection(Map<fieldId, Set<path>>)` carries it (default empty →
existing reads byte-identical); `ParquetReader` gained a backward-compatible constructor overload
that passes it to `ReadConf`; `ReadConf` runs both the physical `projection` and the schema handed
to `readerFunc` through `pruneVariantPaths(…, hint)`, while `typeWithIds` stays intact for
row-group filtering. `readBuilderVariantProjectionReadsOnlyRequestedPath` exercises the full path:
`Parquet.read().withVariantProjection(...)` reconstructs an object with only the requested path.
The vectorized path is unchanged (passes an empty map).

**Remaining (integration with the manifest scan):** `V4ManifestReader` should populate the hint
from the scan filter's `BoundExtract` paths (extending #17433 `projectStats`) so manifest planning
uses it automatically. That is the only piece left, and it lives in the (still-in-flight) v4
manifest read path rather than the Parquet layer, which is now complete.
