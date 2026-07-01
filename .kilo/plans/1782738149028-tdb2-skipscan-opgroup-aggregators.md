# TDB2 Skip-Scan: Generalized OpGroup Aggregator Handling

## Goal

Extend `OpExecutorTDB2SkipScan` (package `org.apache.jena.tdb2.solver.index`) so the
skip-scan optimization for `OpGroup`:

1. Supports all aggregator types whose result is invariant under removing duplicate
   values of a single value expression: every `DISTINCT` single-var aggregator
   (`COUNT`, `SUM`, `AVG`, `MEDIAN`, `MODE`, `SAMPLE`, `GROUP_CONCAT`) plus
   non-distinct `MIN`, `MAX`, `SAMPLE` (distinct is irrelevant for these).
   The single value expression must be a plain `Var`.
2. Supports multiple suitable aggregators in one `OpGroup` by running a separate
   ordered skip-scan per aggregator and combining iterators that share a compatible
   group-by order with an order-aware merge scan.
3. Exposes order information from the skip-scan APIs **before execution**, so the
   OpGroup logic can plan the best combination and fall back if none is suitable.
4. Keeps `OpExecutorTDB2` unchanged (it already only delegates); all new logic lives
   in the `index` package.

## Current Behavior (baseline)

- `OpExecutorTDB2.execute(OpGroup)` (jena-tdb2 `OpExecutorTDB2.java:114`) only delegates
  to `OpExecutorTDB2SkipScan.tryExec(opGroup, input, execCxt)` when skip-scan is enabled.
  **No change needed here.**
- `OpExecutorTDB2SkipScan.tryExec(OpGroup, ...)` (`OpExecutorTDB2SkipScan.java:495`) only
  handles: no group-by expressions, exactly one aggregator, and only
  `AggCountVarDistinct`. It builds `OpDistinct(OpProject(subOp, groupVars+v))`, runs the
  single-pattern skip-scan, then wraps with `QueryIterGroup` using a non-distinct copy.
- `distinctVarOrNull` / `convertToNonDistinct` (`OpExecutorTDB2SkipScan.java:523-541`)
  hardcode `AggCountVarDistinct`.
- `tryExec(NodeTupleTable, distinct, lookup, execCxt)` (`:174`) internally picks the single
  best index via `IndexMatch.COMPARATOR` and executes it, returning a `QueryIterator`.
  The produced sort order (the index key-prefix → variable mapping) is not exposed.
- The skip-scan output order is exactly the chosen index's key order; the leading
  positions covered by group vars define the available group-by order.
- ARQ has no merge/sort-merge join iterator (`sparql/engine/join` only has hash and
  nested-loop). A merge operator must be created here.

## Key Correctness Note (merge semantics)

Each per-aggregator skip-scan yields **distinct `(group-vars, valueVar_i)`** tuples for
its own value var. Different aggregators produce different per-group cardinalities, so
their rows cannot be packed into shared multi-value rows. The merge must therefore
**interleave per-aggregator row streams by equal group-key**, and each emitted row must
carry only its own value var bound (others unbound) so that `QueryIterGroup` accumulates
each aggregator solely over its own distinct values. Converting aggregators to their
non-distinct forms is then correct because each value appears once per group:
`COUNT(DISTINCT v) -> COUNT(v)`, `SUM(DISTINCT v) -> SUM(v)`, `AVG(DISTINCT v) -> AVG(v)`,
`MEDIAN/MODE/SAMPLE/GROUP_CONCAT(DISTINCT) -> non-distinct`, `MIN/MAX/SAMPLE` unchanged.

## Design Decisions (resolved)

- Aggregator scope: all DISTINCT single-var aggregators + non-distinct MIN/MAX/SAMPLE.
- Multi-aggregator: merge-scan when group-by orders are compatible; otherwise return null
  (fall back to default execution). All-or-nothing.
- Order exposure: extend the skip-scan API to expose candidate orders **before**
  execution; OpGroup picks the best combination, then executes.
- Mixed/partial: only handle when group-by is plain vars (no expressions) AND every
  aggregator is suitable; otherwise return null.

## Implementation Tasks

### 1. Aggregator suitability classifier (new helper in index package)

- Add `AggregatorSupport` (or static helpers in `OpExecutorTDB2SkipScan`) that, given an
  `Aggregator`, returns the single value `Var` if the aggregator is skip-scan-suitable,
  else null. Suitable iff:
  - It is one of: `AggCountVarDistinct`, `AggSumDistinct`, `AggAvgDistinct`,
    `AggMedianDistinct`, `AggModeDistinct`, `AggSampleDistinct`, `AggGroupConcatDistinct`,
    `AggMin`, `AggMax`, `AggMinDistinct`, `AggMaxDistinct`, `AggSample` (non-distinct
    MIN/MAX/SAMPLE are distinct-invariant), and
  - its `getExprList()` has exactly one expr that is a `Var`.
  - `AggCountVarDistinct` keeps existing behavior.
- Add `convertToNonDistinct(Aggregator)` covering all of the above:
  use `AggregatorFactory.createCount/createSum/createAvg/createMedian/createMode/
  createSample/createGroupConcat(false, expr[, sep])` for the distinct variants; return
  MIN/MAX/SAMPLE (and already-non-distinct) unchanged. For `GROUP_CONCAT` preserve the
  separator.
- Replace `distinctVarOrNull` and the single-class `convertToNonDistinct` with these.

### 2. Split skip-scan into planning + execution (order exposure)

Refactor `tryExec(NodeTupleTable, distinct, lookup, execCxt)` so candidate orders are
available before execution:

- Add a planning method, e.g.
  `List<SkipScanCandidate> planCandidates(NodeTupleTable, TuplePatternSpec lookup)`
  that iterates indexes, computes `IndexMatch` (reuse `computeIndexMatch`), and returns
  one candidate per usable index. Each `SkipScanCandidate` record holds:
  - the `TupleIndex` and its `IndexMatch`,
  - `boolean canDoDirectDistinct`,
  - the produced variable order: the ordered `List<Var>` corresponding to the index's
    key/value slots that map to projected vars (derive from `TupleMap` + lookup, the same
    information `mapProjection` uses), i.e. the order in which group vars appear in the
    key prefix.
- Add an execution method that, given a chosen `SkipScanCandidate`, performs the existing
  `tryComputeIndexMap` + `exec(...)` + optional `QueryIterDistinct` wrapping and returns a
  `QueryIterator`. Returns the null-iterator on missing NodeIds as today.
- Keep the existing public `tryExec(...)` entrypoints (`PatternQuery`, `OpDistinct`,
  single-pattern) returning `QueryIterator` by internally calling
  `planCandidates` + pick-best (via `IndexMatch.COMPARATOR`) + execute. This preserves the
  current OpDistinct/single-pattern behavior and external API.

### 3. Generalized OpGroup planning

Rewrite `tryExec(OpGroup opGroup, input, execCxt)`:

- Guard: return null unless `getGroupVars().getExprs().isEmpty()` (plain group vars only).
- For each `ExprAggregator`, compute its suitable value `Var` via the classifier; if any
  aggregator is unsuitable, return null (all-or-nothing).
- For each aggregator build its skip-scan lookup: `OpProject(subOp, groupVars + valueVar)`
  under distinct semantics, then `PatternQuery.createOrNull(...)`. If any is null, return null.
- For each aggregator, call `planCandidates(...)` to get candidate (index, produced-order)
  options. The relevant order for grouping is the **group-vars prefix order** each
  candidate produces.
- **Combination search**: find a set of candidates (one per aggregator) maximizing the
  number of aggregators that share a compatible group-by order so they can be merged with
  a single merge-scan. A compatible order means the leading group-by vars appear in the
  same sequence across candidates (so equal-group detection by prefix comparison works).
  - Preferred outcome: a single shared order covering all aggregators (one merge group).
  - Otherwise: partition aggregators into order-groups; each order-group merges
    internally; order-groups are independent sub-results that must still be combined into
    one grouped result (see Task 4).
  - If no candidate exists for some aggregator, return null (fall back).
- Execute the chosen candidate per aggregator (Task 2 execution method), tagging each
  resulting `QueryIterator` with its value var and produced order.

### 4. Order-aware merge-scan operator (new, in index package)

Add a new operator (e.g. `QueryIterMergeScanGroup` or a `QueryIter` subclass) in
`org.apache.jena.tdb2.solver.index`:

- Input: a list of `(QueryIterator iter, Var valueVar)` all ordered by the same group-by
  var sequence, plus the ordered list of group vars.
- Behavior: classic k-way merge by group key. For each distinct group key (compared on the
  group vars in the agreed order), interleave-emit the rows from each input belonging to
  that group, each row binding the group vars plus only that input's value var. Advance all
  inputs past the current group before moving on. Output is grouped contiguously by group
  key, which is what `QueryIterGroup` needs.
- Respect `execCxt.checkCancelSignal()` and proper close/cancel propagation
  (mirror `QueryIterGroup` cancel/close handling).
- Combining across multiple incompatible order-groups: if more than one order-group
  results, either (a) return null for the whole OpGroup in this iteration (simplest,
  still correct via fallback) or (b) reconcile order-groups by re-sorting one group's
  output to the other's order. **Recommended: start with (a)** — only emit a merged result
  when a single compatible order covers all aggregators; otherwise fall back. Document this
  as a known limitation to revisit.

### 5. Wire-up and final grouping

- Build the merged input iterator (Task 4) and wrap it in
  `new QueryIterGroup(mergedIter, opGroup.getGroupVars(), newAggs, execCxt)` where `newAggs`
  are the non-distinct converted `ExprAggregator`s (preserving each aggregator's result var
  via `new ExprAggregator(eAgg.getVar(), convertToNonDistinct(agg))`).
- Single-aggregator path is just the degenerate case of the merge (one input); ensure it
  still produces results identical to the current implementation.

## Files

- Modify: `jena-tdb2/.../tdb2/solver/index/OpExecutorTDB2SkipScan.java`
  (classifier, plan/execute split, new OpGroup logic).
- New: `jena-tdb2/.../tdb2/solver/index/SkipScanCandidate.java` (record: index, IndexMatch,
  canDoDirectDistinct, produced order vars).
- New: `jena-tdb2/.../tdb2/solver/index/QueryIterMergeScanGroup.java` (merge operator).
- Optional new: `jena-tdb2/.../tdb2/solver/index/AggregatorSupport.java`
  (suitability + non-distinct conversion) if not kept inside `OpExecutorTDB2SkipScan`.
- No change to `jena-tdb2/.../tdb2/solver/OpExecutorTDB2.java`.
- `CandidateIndex.java` is currently unused; either reuse/rename for the new candidate
  concept or leave untouched (prefer introducing `SkipScanCandidate` to avoid churn).

## Validation

- Extend `TestOpExecutorTDB2SkipScan` (jena-tdb2 test) with generated tests mirroring the
  existing `createTestsCountVarDistinct` pattern, using `GraphCompareSelectResultExecutable`
  to compare TDB2 skip-scan results against the in-memory reference for:
  - `SUM(DISTINCT ?v)`, `AVG(DISTINCT ?v)`, `MIN(?v)`, `MAX(?v)`, `SAMPLE(?v)` single
    aggregator with group-by permutations (note: `SAMPLE`/non-deterministic results — for
    `SAMPLE` compare only that a valid member is returned, or restrict to deterministic
    aggregators for exact compare; treat SAMPLE as a smoke test).
  - Multiple aggregators with the same group-by, e.g.
    `(COUNT(DISTINCT ?a) AS ?c) (SUM(DISTINCT ?b) AS ?s)` — verify merged result equals
    reference, and that it falls back gracefully when orders are incompatible.
  - Mixed unsuitable aggregator and group-by expression cases return correct results via
    fallback (parity with reference).
- Run focused build/tests:
  `mvn -q -pl jena-tdb2 -am test -Dtest=TestOpExecutorTDB2SkipScan`
  (and the broader TDB2 solver test suite) — the comparison tests are the primary
  correctness gate.
- Sanity: ensure `OpDistinct` single-pattern behavior is unchanged (existing
  `testDistinct` / `testCountVarDistinct` still pass).

## Risks / Open Items

- Determining "compatible group-by order" across candidate indexes must be precise:
  group vars must appear as a contiguous, equally-ordered prefix in each candidate's
  produced order for prefix-based equal-group comparison to be valid.
- Multiple incompatible order-groups are deferred (fall back to default). Revisit if a
  real query needs cross-order reconciliation (would require re-sorting).
- `SAMPLE` is non-deterministic; exact-result comparison tests are not meaningful for it —
  use a structural/smoke check.
- `GROUP_CONCAT` ordering: only the unordered DISTINCT form is in scope; ORDER BY inside
  GROUP_CONCAT is not (and is already unsupported in `AggregatorFactory`).
