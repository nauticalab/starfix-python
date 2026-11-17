# Prepared Schema Implementation Plan (ITL-1177)

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a caller analyse an Arrow schema once (`ArrowDigester.prepare`) and hash many tables or record
batches of that schema without redoing the analysis, with every digest byte-identical to the one-shot calls.

**Architecture:** `_format2.analyse_schema` runs the schema analysis once and returns an immutable
`SchemaAnalysis` (node list, canonical schema key, schema digest, entry specs, the data-support verdict, and
the schema's Arrow IPC bytes for the same-schema check). `Digester` is built from an analysis; the one-shot
entry points build one analysis per call and never re-check their own input. The public
`starfix.PreparedSchema` (in `arrow_digester.py`) wraps one analysis and its schema, owned by the caller.
No module-level, global or cached state is added.

**Tech Stack:** Python >= 3.10, pyarrow >= 19, pytest, uv.

---

## Interface

```python
ArrowDigester.prepare(schema, *, include_metadata=False, ordered_columns=False,
                      ordered_struct_fields=False) -> PreparedSchema

class PreparedSchema:                     # built only by prepare; immutable; pickles by re-preparing
    schema: pa.Schema                     # read-only properties
    include_metadata: bool
    ordered_columns: bool
    ordered_struct_fields: bool
    schema_digest: bytes                  # == ArrowDigester.hash_schema(schema, same options)
    def hash_record_batch(self, record_batch) -> bytes
    def hash_table(self, table) -> bytes
    def digester(self) -> ArrowDigester
```

* `prepare` raises exactly what `hash_schema` raises. Types whose schema hashes but whose data does not
  (Null, Map, Interval, view types) do not make `prepare` fail; `hash_record_batch`, `hash_table` and
  `digester()` raise the existing `ValueError` for them.
* Same-schema rule: accepted iff the input schema's canonical schema key under the prepared options equals
  the prepared one (Phase 1 text; plus Phase 2 text with `include_metadata`), i.e. iff
  `hash_schema(input.schema, options) == prepared.schema_digest`. This is today's rule of
  `ArrowDigester.update`.
* Check: (1) `input.schema.serialize()` bytes equal the prepared schema's IPC bytes -> identical schema ->
  accept; (2) otherwise compute the input's key (today's analysis) and compare; mismatch -> `ValueError`
  ("Record batch schema does not match the prepared schema" / "Table schema does not match the prepared
  schema"; `digester().update` keeps "Record batch schema does not match ArrowDigester schema").
  `pa.Schema.equals(check_metadata=True)` is not used: it ignores nested metadata of dictionary value types
  and the parameters of Python-defined extension types.

## Files

* Modify `src/starfix/_format2.py`: `SchemaAnalysis`, `analyse_schema`, `phase2_json(schema, nodes)`,
  `_Entry(kind, nullable)`, `Digester.from_analysis`, unchecked ingestion for one-shot calls.
* Modify `src/starfix/arrow_digester.py`: `ArrowDigester.prepare`, `PreparedSchema`.
* Modify `src/starfix/__init__.py`: export `PreparedSchema`.
* Create `tests/random_arrow.py`: seeded random schemas and data (nested, dictionary, fixed-size list,
  extension types, metadata, empty and sliced inputs).
* Create `tests/test_prepared_schema.py`: API, immutability, no module state, golden vectors x all option
  combinations, the type matrix, accept/refuse rule, seeded differential, analysis-count tests.
* Create `scripts/release_differential.py`: this checkout (one-shot and prepared) against the released
  `starfix==0.5.0` from PyPI, byte-identical. Add a CI job for it beside `legacy-differential`.
* Create `scripts/bench_prepared.py`: time per one-row hash, one-shot and prepared.
* Modify `README.md` (usage + same-schema rule), `CHANGELOG.md` (Unreleased, proposed 0.6.0).

## Tasks

### Task 1: failing tests for the public interface
- [ ] Write `tests/test_prepared_schema.py` tests: `prepare` returns a `PreparedSchema` whose properties
  echo the options; `schema_digest == hash_schema` for all 8 option combinations; `PreparedSchema()` raises
  `TypeError`; setting/deleting attributes raises `AttributeError`; pickle round trip hashes the same;
  `hash_record_batch` / `hash_table` / `digester()` equal the one-shot results.
- [ ] Run `uv run pytest tests/test_prepared_schema.py -q`; expect failures (no `prepare`).

### Task 2: analysis once (refactor, no behaviour change)
- [ ] `analyse_schema` computes nodes once, Phase 1 once, Phase 2 once (from the same nodes), the digest
  from that key, sorted entry specs, the data-support verdict and the IPC bytes.
- [ ] `Digester.__init__` = `analyse_schema` + `from_analysis`; `update` checks via the analysis;
  `hash_record_batch` / `hash_table` analyse once and ingest without re-checking (a table's batches carry
  the table's schema).
- [ ] Test: counting wrappers on `schema_nodes`, `phase1_json`, `phase2_json` show one call each per
  one-shot `hash_table` / `hash_record_batch`, and none for a prepared hash of an identical schema.
- [ ] Run the full suite: `uv run pytest tests/ -q` (golden vectors pin the bytes).

### Task 3: `PreparedSchema` and `ArrowDigester.prepare`
- [ ] Implement in `arrow_digester.py`; export from `starfix`. Run Task 1 tests: pass.

### Task 4: same-schema rule
- [ ] Refusal cases (each with every relevant option combination): column name, type, nullability (top
  and nested), column order with `ordered_columns`, struct field order with `ordered_struct_fields`, field
  metadata at top level, in a struct child, a list / fixed-size-list element, the struct inside a dictionary
  value, schema metadata, a Python extension type's parameter, extension vs storage type, all with
  `include_metadata`. Acceptance cases: string vs large_string, dictionary vs plain, reordered columns or
  struct fields when unordered, metadata differences without `include_metadata`; accepted results equal the
  one-shot hash of the input.
- [ ] Seeded pairs: accepted iff `hash_schema` digests are equal.

### Task 5: differential proofs
- [ ] Golden vectors (`tests/golden/format2_v0.5.json`) x 8 option combinations: prepared == one-shot;
  for the vector's own options == expected.
- [ ] Type matrix (`tests/test_format2_type_matrix.py::_hashes`) also through a prepared schema.
- [ ] Seeded random inputs (`tests/random_arrow.py`), all option combinations, batch / table / stream.
- [ ] `scripts/release_differential.py` against `starfix==0.5.0`; CI job.

### Task 6: benchmark, docs, changelog, PR
- [ ] `scripts/bench_prepared.py`; numbers before (origin/main) and after.
- [ ] README section "Hashing many inputs of one schema"; CHANGELOG Unreleased.
- [ ] `uv run pytest tests/ -q`; commit (conventional commits); PR against `main`.
