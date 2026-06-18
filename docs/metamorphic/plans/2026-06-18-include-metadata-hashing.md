# `include_metadata` Hashing Option — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `include_metadata: bool = False` to `ArrowDigester`'s schema/batch/table hashing entry points, implementing the same two-phase algorithm as the Rust `starfix` crate.

**Architecture:** Three new private helpers (`_sort_metadata`, `_collect_nested_field_metadata`, `_update_metadata_hash`) are added to `arrow_digester.py`. `_hash_schema` is updated to accept `include_metadata`. Five `ArrowDigester` methods gain the kwarg. All changes are in two files only — no new modules.

**Tech Stack:** Python 3.12, PyArrow ≥14.0, pytest, hashlib (stdlib), json (stdlib). Test runner: `uv run pytest`.

**Spec:** `docs/metamorphic/specs/2026-06-18-include-metadata-hashing-design.md`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/starfix/arrow_digester.py` | Modify | Three new private helpers; `_hash_schema`, `ArrowDigester.__init__`, `hash_schema`, `hash_record_batch`, `hash_table` updated; class docstring updated |
| `tests/test_metadata_hashing.py` | Create | All metadata hashing tests |
| `README.md` | Modify | "Metadata hashing" section |

---

### Task 0: Create feature branch

- [ ] **Step 1: Create and check out the branch**

```bash
cd /home/kurouto/kurouto-jobs/e0ee6da3-e9e9-431f-872d-9297756b135b/starfix-python
git checkout -b eywalker/plt-1734-starfix-python-expose-schemafield-metadata-hashing-option
```

- [ ] **Step 2: Verify branch**

```bash
git branch --show-current
```

Expected output: `eywalker/plt-1734-starfix-python-expose-schemafield-metadata-hashing-option`

---

### Task 1: `_sort_metadata` and `_collect_nested_field_metadata` helpers

**Files:**
- Modify: `src/starfix/arrow_digester.py` (insert after existing `_hash_schema` at line 197)
- Create: `tests/test_metadata_hashing.py`

- [ ] **Step 1: Create test file with unit tests for both helpers**

Create `tests/test_metadata_hashing.py`:

```python
"""Tests for include_metadata hashing option (PLT-1734).

Verifies that:
- include_metadata=False (default) ignores all Arrow metadata (hash format 0.0.1 stability)
- include_metadata=True detects changes to schema-level and field-level metadata
- Hashes are deterministic regardless of metadata key insertion order
- Empty-metadata invariant: no metadata → same hash regardless of include_metadata
"""
from __future__ import annotations

import pyarrow as pa
import pytest
from starfix.arrow_digester import (
    ArrowDigester,
    _collect_nested_field_metadata,
    _sort_metadata,
)


class TestSortMetadata:
    def test_sorts_by_key(self):
        meta = {b"z": b"1", b"a": b"2", b"m": b"3"}
        result = _sort_metadata(meta)
        assert list(result.keys()) == ["a", "m", "z"]

    def test_decodes_bytes_to_utf8(self):
        meta = {b"key": b"value"}
        result = _sort_metadata(meta)
        assert result == {"key": "value"}
        assert isinstance(list(result.keys())[0], str)

    def test_none_returns_empty_dict(self):
        assert _sort_metadata(None) == {}

    def test_empty_dict_returns_empty_dict(self):
        assert _sort_metadata({}) == {}


class TestCollectNestedFieldMetadata:
    def test_top_level_field_with_metadata(self):
        field = pa.field("x", pa.int32(), metadata={b"unit": b"kg"})
        result = _collect_nested_field_metadata(field, "x")
        assert result == {"x": {"unit": "kg"}}

    def test_field_without_metadata_not_included(self):
        field = pa.field("x", pa.int32())
        result = _collect_nested_field_metadata(field, "x")
        assert result == {}

    def test_struct_child_metadata_uses_slash_separator(self):
        child = pa.field("age", pa.int32(), metadata={b"unit": b"years"})
        parent = pa.field("person", pa.struct([child]))
        result = _collect_nested_field_metadata(parent, "person")
        assert result == {"person/age": {"unit": "years"}}

    def test_list_element_field_uses_trailing_slash(self):
        element = pa.field("item", pa.int32(), metadata={b"unit": b"count"})
        list_field = pa.field("items", pa.large_list(element))
        result = _collect_nested_field_metadata(list_field, "items")
        assert result == {"items/": {"unit": "count"}}

    def test_regular_list_element_field_also_uses_trailing_slash(self):
        element = pa.field("item", pa.int32(), metadata={b"unit": b"count"})
        list_field = pa.field("items", pa.list_(element))
        result = _collect_nested_field_metadata(list_field, "items")
        assert result == {"items/": {"unit": "count"}}

    def test_parent_field_and_child_both_with_metadata(self):
        child = pa.field("age", pa.int32(), metadata={b"unit": b"years"})
        parent = pa.field("person", pa.struct([child]), metadata={b"source": b"census"})
        result = _collect_nested_field_metadata(parent, "person")
        assert result == {
            "person": {"source": "census"},
            "person/age": {"unit": "years"},
        }

    def test_result_is_sorted_by_path(self):
        child_z = pa.field("z", pa.int32(), metadata={b"k": b"v"})
        child_a = pa.field("a", pa.int32(), metadata={b"k": b"v"})
        parent = pa.field("s", pa.struct([child_z, child_a]))
        result = _collect_nested_field_metadata(parent, "s")
        assert list(result.keys()) == ["s/a", "s/z"]
```

- [ ] **Step 2: Run to confirm ImportError**

```bash
cd /home/kurouto/kurouto-jobs/e0ee6da3-e9e9-431f-872d-9297756b135b/starfix-python
uv run pytest tests/test_metadata_hashing.py -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name '_sort_metadata' from 'starfix.arrow_digester'`

- [ ] **Step 3: Add helpers to `arrow_digester.py` after `_hash_schema` (line 197)**

In `src/starfix/arrow_digester.py`, insert after line 197 (the `_hash_schema` function):

```python
# ---------------------------------------------------------------------------
# Metadata helpers  (Phase 2 schema hashing — PLT-1734)
# ---------------------------------------------------------------------------

def _sort_metadata(metadata) -> dict[str, str]:
    """Decode PyArrow metadata (bytes → bytes) to a sorted {str: str} dict.

    Returns an empty dict when metadata is None or empty.
    Arrow IPC spec guarantees metadata keys and values are valid UTF-8.
    """
    if not metadata:
        return {}
    return dict(sorted(
        (k.decode("utf-8") if isinstance(k, bytes) else k,
         v.decode("utf-8") if isinstance(v, bytes) else v)
        for k, v in metadata.items()
    ))


def _collect_nested_field_metadata(
    field: pa.Field, path: str
) -> dict[str, dict[str, str]]:
    """Recursively collect metadata from a field and its nested children.

    Returns a dict sorted by path: {path: sorted_metadata_dict}.
    Only fields with non-empty metadata are included.

    Path convention (matches the data-hashing BTreeMap paths):
    - Struct children:         "{parent_path}/{child_name}"
    - List/LargeList element:  "{parent_path}/"  (trailing slash, no element name)
    """
    import pyarrow as pa

    result: dict[str, dict[str, str]] = {}

    if field.metadata:
        result[path] = _sort_metadata(field.metadata)

    if pa.types.is_struct(field.type):
        for i in range(field.type.num_fields):
            child = field.type.field(i)
            child_path = f"{path}{DELIMITER}{child.name}"
            result.update(_collect_nested_field_metadata(child, child_path))
    elif (
        pa.types.is_list(field.type)
        or pa.types.is_large_list(field.type)
        or pa.types.is_fixed_size_list(field.type)
    ):
        element_path = f"{path}{DELIMITER}"
        result.update(_collect_nested_field_metadata(field.type.value_field, element_path))

    return dict(sorted(result.items()))
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_metadata_hashing.py::TestSortMetadata tests/test_metadata_hashing.py::TestCollectNestedFieldMetadata -v
```

Expected: 11 passed

- [ ] **Step 5: Commit**

```bash
git add src/starfix/arrow_digester.py tests/test_metadata_hashing.py
git commit -m "feat: add _sort_metadata and _collect_nested_field_metadata helpers (PLT-1734)"
```

---

### Task 2: `_update_metadata_hash` and two-phase `_hash_schema`

**Files:**
- Modify: `src/starfix/arrow_digester.py` (insert after `_collect_nested_field_metadata`; replace `_hash_schema`)

- [ ] **Step 1: Add `TestEmptyMetadataInvariant` seed test to the test file**

Append to `tests/test_metadata_hashing.py`:

```python
class TestEmptyMetadataInvariant:
    def test_no_metadata_include_true_equals_false(self):
        """Schema with no metadata: include_metadata=True == include_metadata=False."""
        schema = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        assert (
            ArrowDigester.hash_schema(schema, include_metadata=True)
            == ArrowDigester.hash_schema(schema, include_metadata=False)
        )
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_metadata_hashing.py::TestEmptyMetadataInvariant -v 2>&1 | head -15
```

Expected: `TypeError: ArrowDigester.hash_schema() got an unexpected keyword argument 'include_metadata'`

- [ ] **Step 3: Add `_update_metadata_hash` after `_collect_nested_field_metadata`**

In `src/starfix/arrow_digester.py`, insert after `_collect_nested_field_metadata`:

```python
def _update_metadata_hash(hasher, schema: pa.Schema) -> None:
    """Feed Phase 2 metadata JSON into ``hasher`` (only when metadata is present).

    Builds a compact JSON string of the form:
      {"fields": {"<path>": {"<key>": "<val>", ...}, ...}, "schema": {"<key>": "<val>", ...}}

    ``"fields"`` is omitted when no field (at any nesting level) has metadata.
    ``"schema"`` is omitted when the schema has no metadata.
    Nothing is written when both would be absent (empty-metadata invariant).
    """
    # Collect all field metadata recursively; result is sorted by path
    all_field_meta: dict[str, dict[str, str]] = {}
    for i in range(len(schema)):
        field = schema.field(i)
        all_field_meta.update(_collect_nested_field_metadata(field, field.name))

    meta_doc: dict[str, object] = {}
    if all_field_meta:
        meta_doc["fields"] = all_field_meta
    if schema.metadata:
        meta_doc["schema"] = _sort_metadata(schema.metadata)

    if meta_doc:
        hasher.update(json.dumps(meta_doc, separators=(",", ":")).encode())
```

- [ ] **Step 4: Replace `_hash_schema` with two-phase version**

Replace the existing `_hash_schema` (currently at line ~196):

```python
def _hash_schema(schema: pa.Schema, include_metadata: bool = False) -> bytes:
    h = hashlib.sha256()
    h.update(_serialized_schema(schema).encode())
    if include_metadata:
        _update_metadata_hash(h, schema)
    return h.digest()
```

- [ ] **Step 5: Run seed test to confirm it passes**

```bash
uv run pytest tests/test_metadata_hashing.py::TestEmptyMetadataInvariant -v
```

Expected: 1 passed

- [ ] **Step 6: Run full existing test suite to confirm no regressions**

```bash
uv run pytest tests/test_golden_parity.py tests/test_golden_parity_r2.py tests/test_arrow_digester.py -q
```

Expected: all pass (the `_hash_schema` default change is backward-compatible)

- [ ] **Step 7: Commit**

```bash
git add src/starfix/arrow_digester.py tests/test_metadata_hashing.py
git commit -m "feat: add _update_metadata_hash and two-phase _hash_schema (PLT-1734)"
```

---

### Task 3: Update `ArrowDigester` API

**Files:**
- Modify: `src/starfix/arrow_digester.py` (`ArrowDigester` class — `__init__`, `hash_schema`, `hash_record_batch`, `hash_table`)

- [ ] **Step 1: Add `TestFieldMetadataChangesHash` to the test file**

Append to `tests/test_metadata_hashing.py`:

```python
class TestFieldMetadataChangesHash:
    def test_hash_schema_detects_field_metadata(self):
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_meta = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"unit": b"kg"}),
        ])
        assert (
            ArrowDigester.hash_schema(schema_plain, include_metadata=True)
            != ArrowDigester.hash_schema(schema_meta, include_metadata=True)
        )

    def test_hash_record_batch_detects_field_metadata(self):
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_meta = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"unit": b"kg"}),
        ])
        batch_plain = pa.record_batch({"x": pa.array([1, 2], type=pa.int32())}, schema=schema_plain)
        batch_meta = pa.record_batch({"x": pa.array([1, 2], type=pa.int32())}, schema=schema_meta)
        assert (
            ArrowDigester.hash_record_batch(batch_plain, include_metadata=True)
            != ArrowDigester.hash_record_batch(batch_meta, include_metadata=True)
        )

    def test_hash_table_detects_field_metadata(self):
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_meta = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"unit": b"kg"}),
        ])
        table_plain = pa.table({"x": pa.array([1, 2], type=pa.int32())}, schema=schema_plain)
        table_meta = pa.table({"x": pa.array([1, 2], type=pa.int32())}, schema=schema_meta)
        assert (
            ArrowDigester.hash_table(table_plain, include_metadata=True)
            != ArrowDigester.hash_table(table_meta, include_metadata=True)
        )

    def test_streaming_digester_detects_field_metadata(self):
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_meta = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"unit": b"kg"}),
        ])
        batch_plain = pa.record_batch({"x": pa.array([1, 2], type=pa.int32())}, schema=schema_plain)
        batch_meta = pa.record_batch({"x": pa.array([1, 2], type=pa.int32())}, schema=schema_meta)

        d_plain = ArrowDigester(schema_plain, include_metadata=True)
        d_plain.update(batch_plain)

        d_meta = ArrowDigester(schema_meta, include_metadata=True)
        d_meta.update(batch_meta)

        assert d_plain.finalize() != d_meta.finalize()

    def test_struct_child_metadata_changes_hash(self):
        child_plain = pa.field("age", pa.int32(), nullable=False)
        child_meta = pa.field("age", pa.int32(), nullable=False, metadata={b"unit": b"years"})
        schema_plain = pa.schema([pa.field("person", pa.struct([child_plain]), nullable=False)])
        schema_meta = pa.schema([pa.field("person", pa.struct([child_meta]), nullable=False)])
        assert (
            ArrowDigester.hash_schema(schema_plain, include_metadata=True)
            != ArrowDigester.hash_schema(schema_meta, include_metadata=True)
        )

    def test_list_element_field_metadata_changes_hash(self):
        element_plain = pa.field("item", pa.int32(), nullable=False)
        element_meta = pa.field("item", pa.int32(), nullable=False, metadata={b"unit": b"count"})
        schema_plain = pa.schema([pa.field("items", pa.large_list(element_plain), nullable=True)])
        schema_meta = pa.schema([pa.field("items", pa.large_list(element_meta), nullable=True)])
        assert (
            ArrowDigester.hash_schema(schema_plain, include_metadata=True)
            != ArrowDigester.hash_schema(schema_meta, include_metadata=True)
        )
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_metadata_hashing.py::TestFieldMetadataChangesHash -v 2>&1 | head -15
```

Expected: `TypeError: ArrowDigester.hash_schema() got an unexpected keyword argument 'include_metadata'`

- [ ] **Step 3: Replace `ArrowDigester.__init__`**

In `src/starfix/arrow_digester.py`, replace the existing `__init__`:

```python
def __init__(self, schema: pa.Schema, *, include_metadata: bool = False) -> None:
    """Initialize a streaming Arrow hasher.

    Args:
        schema: The Arrow schema all record batches must conform to.
        include_metadata: When True, schema-level and per-field Arrow
            metadata are included in the hash. Default is False,
            preserving hash format 0.0.1 stability. A schema with no
            metadata produces the same hash regardless of this flag
            (empty-metadata invariant).
    """
    self._schema = schema
    self._include_metadata = include_metadata
    self._schema_digest = _hash_schema(schema, include_metadata=include_metadata)
    # BTreeMap<path, (BitVec|None, sha256|None, sha256|None)> — sorted by key
    self._fields: dict[str, tuple] = {}
    for i in range(len(schema)):
        _extract_fields(schema.field(i), "", self._fields)
    # Ensure sorted order (Python 3.7+ dicts are insertion-ordered)
    self._fields = dict(sorted(self._fields.items()))
```

- [ ] **Step 4: Replace `hash_schema`, `hash_record_batch`, `hash_table` static methods**

In `src/starfix/arrow_digester.py`, replace the three static methods:

```python
@staticmethod
def hash_schema(schema: pa.Schema, *, include_metadata: bool = False) -> bytes:
    """Hash an Arrow schema.

    Args:
        schema: The schema to hash.
        include_metadata: When True, schema-level and per-field Arrow
            metadata are included in the hash. Default is False,
            preserving hash format 0.0.1 stability.
    """
    return VERSION_BYTES + _hash_schema(schema, include_metadata=include_metadata)

@staticmethod
def hash_record_batch(record_batch: pa.RecordBatch, *, include_metadata: bool = False) -> bytes:
    """Hash an Arrow record batch.

    Args:
        record_batch: The record batch to hash.
        include_metadata: When True, schema-level and per-field Arrow
            metadata are included in the hash. Default is False,
            preserving hash format 0.0.1 stability.
    """
    d = ArrowDigester(record_batch.schema, include_metadata=include_metadata)
    d.update(record_batch)
    return d.finalize()

@staticmethod
def hash_table(table: pa.Table, *, include_metadata: bool = False) -> bytes:
    """Hash a full Arrow table (iterates over all batches).

    Args:
        table: The table to hash.
        include_metadata: When True, schema-level and per-field Arrow
            metadata are included in the hash. Default is False,
            preserving hash format 0.0.1 stability.
    """
    d = ArrowDigester(table.schema, include_metadata=include_metadata)
    for batch in table.to_batches():
        d.update(batch)
    return d.finalize()
```

- [ ] **Step 5: Run new tests to confirm they pass**

```bash
uv run pytest tests/test_metadata_hashing.py::TestFieldMetadataChangesHash tests/test_metadata_hashing.py::TestEmptyMetadataInvariant -v
```

Expected: all pass

- [ ] **Step 6: Run full test suite to confirm no regressions**

```bash
uv run pytest tests/ -q
```

Expected: all tests pass

- [ ] **Step 7: Commit**

```bash
git add src/starfix/arrow_digester.py tests/test_metadata_hashing.py
git commit -m "feat: add include_metadata kwarg to ArrowDigester API (PLT-1734)"
```

---

### Task 4: Complete behavioral test suite

**Files:**
- Modify: `tests/test_metadata_hashing.py`

- [ ] **Step 1: Append remaining test classes to `tests/test_metadata_hashing.py`**

```python
class TestMetadataExcludedByDefault:
    def test_field_metadata_ignored_by_default(self):
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_meta = pa.schema([
            pa.field("x", pa.int32(), nullable=False,
                     metadata={b"ARROW:extension:name": b"my_ext"}),
        ])
        assert ArrowDigester.hash_schema(schema_plain) == ArrowDigester.hash_schema(schema_meta)

    def test_schema_metadata_ignored_by_default(self):
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_meta = pa.schema(
            [pa.field("x", pa.int32(), nullable=False)],
            metadata={b"version": b"2"},
        )
        assert ArrowDigester.hash_schema(schema_plain) == ArrowDigester.hash_schema(schema_meta)

    def test_default_matches_hash_format_001_golden_value(self):
        """Batch with metadata + include_metadata=False must match the hash format 0.0.1
        golden value from test_golden_parity.py::TestSpecExamples::test_example_a."""
        schema = pa.schema([
            pa.field("age", pa.int32(), nullable=False, metadata={b"unit": b"years"}),
            pa.field("name", pa.large_utf8(), nullable=True),
        ], metadata={b"source": b"survey"})
        batch = pa.record_batch(
            {
                "age": pa.array([25, 30], type=pa.int32()),
                "name": pa.array(["Alice", None], type=pa.large_utf8()),
            },
            schema=schema,
        )
        result = ArrowDigester.hash_record_batch(batch, include_metadata=False).hex()
        assert result == "0000018020e47f4462f26b0bc73ad110ea0f9198c2745c04ce23335093d2b78ef51c88"


class TestSchemaMetadataChangesHash:
    def test_schema_level_metadata_changes_hash(self):
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_meta = pa.schema(
            [pa.field("x", pa.int32(), nullable=False)],
            metadata={b"version": b"2"},
        )
        assert (
            ArrowDigester.hash_schema(schema_plain, include_metadata=True)
            != ArrowDigester.hash_schema(schema_meta, include_metadata=True)
        )

    def test_field_and_schema_metadata_independently_encoded(self):
        """Same key/value placed on a field vs on the schema must produce different hashes."""
        schema_on_field = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"version": b"2"}),
        ])
        schema_on_schema = pa.schema(
            [pa.field("x", pa.int32(), nullable=False)],
            metadata={b"version": b"2"},
        )
        assert (
            ArrowDigester.hash_schema(schema_on_field, include_metadata=True)
            != ArrowDigester.hash_schema(schema_on_schema, include_metadata=True)
        )


class TestMetadataDeterminism:
    def test_field_metadata_key_order_does_not_affect_hash(self):
        meta_abc = {b"alpha": b"1", b"beta": b"2", b"gamma": b"3"}
        meta_cba = {b"gamma": b"3", b"alpha": b"1", b"beta": b"2"}
        schema_a = pa.schema([pa.field("x", pa.int32(), nullable=False, metadata=meta_abc)])
        schema_b = pa.schema([pa.field("x", pa.int32(), nullable=False, metadata=meta_cba)])
        assert (
            ArrowDigester.hash_schema(schema_a, include_metadata=True)
            == ArrowDigester.hash_schema(schema_b, include_metadata=True)
        )

    def test_schema_metadata_key_order_does_not_affect_hash(self):
        schema_a = pa.schema([pa.field("x", pa.int32())], metadata={b"p": b"1", b"q": b"2"})
        schema_b = pa.schema([pa.field("x", pa.int32())], metadata={b"q": b"2", b"p": b"1"})
        assert (
            ArrowDigester.hash_schema(schema_a, include_metadata=True)
            == ArrowDigester.hash_schema(schema_b, include_metadata=True)
        )

    def test_multiple_fields_with_shuffled_metadata_keys(self):
        schema_a = pa.schema([
            pa.field("x", pa.int32(), metadata={b"p": b"1", b"q": b"2", b"r": b"3"}),
            pa.field("y", pa.float64(), metadata={b"s": b"4", b"t": b"5", b"u": b"6"}),
        ])
        schema_b = pa.schema([
            pa.field("x", pa.int32(), metadata={b"r": b"3", b"p": b"1", b"q": b"2"}),
            pa.field("y", pa.float64(), metadata={b"u": b"6", b"s": b"4", b"t": b"5"}),
        ])
        assert (
            ArrowDigester.hash_schema(schema_a, include_metadata=True)
            == ArrowDigester.hash_schema(schema_b, include_metadata=True)
        )


class TestEmptyMetadataInvariantFull:
    def test_explicit_empty_schema_metadata_treated_as_no_metadata(self):
        schema_none = pa.schema([pa.field("x", pa.int32())])
        schema_empty = pa.schema([pa.field("x", pa.int32())], metadata={})
        assert (
            ArrowDigester.hash_schema(schema_none, include_metadata=True)
            == ArrowDigester.hash_schema(schema_empty, include_metadata=True)
        )


class TestRoundTrip:
    def test_adding_metadata_changes_hash(self):
        schema_before = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_after = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"unit": b"kg"}),
        ])
        assert (
            ArrowDigester.hash_schema(schema_before, include_metadata=True)
            != ArrowDigester.hash_schema(schema_after, include_metadata=True)
        )

    def test_removing_metadata_restores_hash(self):
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_meta = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"unit": b"kg"}),
        ])
        h_plain = ArrowDigester.hash_schema(schema_plain, include_metadata=True)
        h_meta = ArrowDigester.hash_schema(schema_meta, include_metadata=True)
        assert h_plain != h_meta
        # Removing metadata restores the original hash
        schema_restored = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        assert ArrowDigester.hash_schema(schema_restored, include_metadata=True) == h_plain

    def test_changing_metadata_value_changes_hash(self):
        schema_v1 = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"version": b"1"}),
        ])
        schema_v2 = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"version": b"2"}),
        ])
        assert (
            ArrowDigester.hash_schema(schema_v1, include_metadata=True)
            != ArrowDigester.hash_schema(schema_v2, include_metadata=True)
        )
```

- [ ] **Step 2: Run all metadata tests to confirm they pass**

```bash
uv run pytest tests/test_metadata_hashing.py -v
```

Expected: all tests pass

- [ ] **Step 3: Run full test suite**

```bash
uv run pytest tests/ -q
```

Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add tests/test_metadata_hashing.py
git commit -m "test: complete metadata hashing test suite (PLT-1734)"
```

---

### Task 5: Documentation — class docstring and README

**Files:**
- Modify: `src/starfix/arrow_digester.py` (class docstring)
- Modify: `README.md`

- [ ] **Step 1: Update `ArrowDigester` class docstring**

In `src/starfix/arrow_digester.py`, replace the existing class docstring:

```python
class ArrowDigester:
    """Pure-Python equivalent of the Rust ``ArrowDigester``.

    Produces identical SHA-256 hashes with a 3-byte version prefix
    (hash format version 0.0.1, independent of the package version).

    By default, Arrow schema- and field-level metadata are excluded from
    the hash. Pass ``include_metadata=True`` to any entry point to include
    them — see the ``include_metadata`` parameter on each method.
    A schema with no metadata produces the same hash regardless of that
    flag (empty-metadata invariant).
    """
```

- [ ] **Step 2: Add "Metadata hashing" section to README.md**

In `README.md`, insert after the closing ` ``` ` of the "Usage" code block and before the "## License" line:

```markdown
## Metadata hashing

By default, Arrow schema- and field-level metadata are excluded from the hash,
preserving hash format 0.0.1 stability. Pass `include_metadata=True` to any
entry point to include them:

```python
# One-shot
digest = ArrowDigester.hash_table(table, include_metadata=True)

# Streaming
digester = ArrowDigester(schema, include_metadata=True)
for batch in batches:
    digester.update(batch)
digest = digester.finalize()
```

When `include_metadata=True`, adding or changing any metadata key or value on
any field (including nested struct children and list element fields) produces a
different hash. Metadata key ordering is deterministic — the hash is stable
regardless of insertion order.

A schema with no metadata produces the same hash regardless of `include_metadata`
(empty-metadata invariant).
```

- [ ] **Step 3: Run full test suite one final time**

```bash
uv run pytest tests/ -q
```

Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add src/starfix/arrow_digester.py README.md
git commit -m "docs: add include_metadata docstrings and README section (PLT-1734)"
```
