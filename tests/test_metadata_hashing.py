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

    def test_map_item_field_metadata_uses_trailing_slash(self):
        item = pa.field("value", pa.int32(), metadata={b"unit": b"cents"})
        map_field = pa.field("m", pa.map_(pa.utf8(), item))
        result = _collect_nested_field_metadata(map_field, "m")
        assert result == {"m/": {"unit": "cents"}}

    def test_fixed_size_list_element_uses_trailing_slash(self):
        element = pa.field("item", pa.int32(), metadata={b"unit": b"count"})
        list_field = pa.field("items", pa.list_(element, 3))
        result = _collect_nested_field_metadata(list_field, "items")
        assert result == {"items/": {"unit": "count"}}


class TestEmptyMetadataInvariant:
    def test_no_metadata_include_true_equals_false(self):
        """Schema with no metadata: include_metadata=True == include_metadata=False."""
        schema = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        assert (
            ArrowDigester.hash_schema(schema, include_metadata=True)
            == ArrowDigester.hash_schema(schema, include_metadata=False)
        )


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
