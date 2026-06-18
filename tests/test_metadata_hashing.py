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
