# Design: `include_metadata` hashing option for starfix-python

**Issue:** PLT-1734
**Date:** 2026-06-18
**Status:** Approved

## Overview

starfix-python is a pure-Python reimplementation of the Rust `starfix` Arrow logical hasher.
PLT-1733 added a two-phase schema hashing algorithm to the Rust crate that optionally includes
Arrow schema- and field-level metadata in the hash. This spec describes how to surface the same
option in the Python implementation with byte-for-byte parity to the Rust output.

## Goals

- Add `include_metadata: bool = False` to the relevant `ArrowDigester` entry points.
- Default (`False`) preserves v0.1.0 hash stability — all existing golden tests continue to pass.
- `True` detects any change to schema-level or field-level Arrow metadata.
- Hash is deterministic regardless of metadata key insertion order.
- Python output is byte-for-byte identical to the Rust `starfix` crate when
  `include_metadata=True`.

## Out of scope

- `hash_array` — standalone arrays carry no schema or field metadata; `include_metadata` is
  intentionally absent here, matching the Rust API.
- Finer-grained metadata control (per-field opt-in, key filtering) — future work.
- Upstream consumer migration (e.g. Orcapod) — separate issue.
- Releasing v0.2.0 — handled separately after this PR merges.

## Algorithm

Schema hashing runs in two phases on a single SHA-256 hasher.

### Phase 1 (always — identical to v0.1.0)

```
hasher.update(_serialized_schema(schema).encode("utf-8"))
```

`_serialized_schema` is unchanged: compact JSON of `{field_name: {data_type, nullable}}`,
keys sorted, types canonicalized.

### Phase 2 (only when `include_metadata=True`)

```
hasher.update(metadata_json.encode("utf-8"))
```

`metadata_json` is the compact JSON serialization of a dict with up to two keys:

```json
{
  "fields": {
    "<field_path>": { "<meta_key>": "<meta_value>", ... },
    ...
  },
  "schema": { "<meta_key>": "<meta_value>", ... }
}
```

Rules:
- `"fields"` is present only when at least one field (at any nesting level) has non-empty metadata.
- `"schema"` is present only when the schema itself has non-empty metadata.
- If both would be absent, Phase 2 writes nothing (**empty-metadata invariant** — a schema with
  no metadata produces the same hash regardless of `include_metadata`).
- All metadata dicts are sorted by key before serialization (determinism).
- Field paths are sorted alphabetically (`"fields"` value is a `BTreeMap`-equivalent).
- PyArrow metadata bytes are decoded to UTF-8 strings (Arrow IPC spec guarantees valid UTF-8).
- JSON is compact (`separators=(",", ":")`), matching Rust's `serde_json::to_string`.

### Field path convention

Field paths in the `"fields"` map follow the same convention as the data-hashing BTreeMap:

| Field location | Path |
|---|---|
| Top-level field `"col"` | `"col"` |
| Struct child `"child"` of top-level `"s"` | `"s/child"` |
| List element field of top-level `"tags"` | `"tags/"` (trailing slash, no element field name) |
| Struct child `"x"` inside list element of `"items"` | `"items//x"` |

This is implemented by `_collect_nested_field_metadata(field, path)`:
- If `field.metadata` is non-empty: add `{path: sorted_kv_dict}` to result.
- If `field.type` is Struct: recurse over each child with path `"{path}/{child_name}"`.
- If `field.type` is List / LargeList / FixedSizeList / Map: recurse over the element field
  with path `"{path}/"` (trailing slash; element field name is omitted).
- Otherwise (leaf type): no recursion.

## API changes

### `ArrowDigester.__init__`

```python
def __init__(self, schema: pa.Schema, *, include_metadata: bool = False) -> None:
```

`include_metadata` is keyword-only. Stored as `self._include_metadata`; passed to `_hash_schema`.

### Static methods

```python
@staticmethod
def hash_schema(schema: pa.Schema, *, include_metadata: bool = False) -> bytes: ...

@staticmethod
def hash_record_batch(record_batch: pa.RecordBatch, *, include_metadata: bool = False) -> bytes: ...

@staticmethod
def hash_table(table: pa.Table, *, include_metadata: bool = False) -> bytes: ...
```

`hash_array` — **no change** (matches Rust API).

All existing call sites remain backward compatible (keyword-only, default `False`).

## New private helpers

| Function | Purpose |
|---|---|
| `_collect_nested_field_metadata(field, path)` | Recursively collect `{path: sorted_meta}` for all fields with metadata |
| `_sort_metadata(metadata)` | Decode bytes keys/values to UTF-8 and sort by key |
| `_update_metadata_hash(hasher, schema)` | Build the Phase 2 JSON and feed it into `hasher` |

## Tests

New file: `tests/test_metadata_hashing.py`.

| Class | What it checks |
|---|---|
| `TestMetadataExcludedByDefault` | `include_metadata=False` ignores metadata; uses a golden value from `test_golden_parity.py` |
| `TestFieldMetadataChangesHash` | `True` detects field-level metadata changes across `hash_schema`, `hash_record_batch`, `hash_table` |
| `TestSchemaMetadataChangesHash` | `True` detects schema-level metadata changes; field vs schema metadata are independently encoded |
| `TestMetadataDeterminism` | Same keys, different insertion order → same hash; multiple fields with shuffled keys |
| `TestEmptyMetadataInvariant` | No metadata: `True` == `False`; explicit empty dict: same invariant |
| `TestRoundTrip` | Add metadata → hash changes; remove → restores; nested field and list element field path convention |

## Documentation changes

- Docstrings on `__init__`, `hash_schema`, `hash_record_batch`, `hash_table` gain an
  `include_metadata` param description.
- README gets a short "Metadata hashing" section after "Usage" showing the kwarg and
  explaining the empty-metadata invariant.
