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
        golden value from test_golden_parity.py::TestSpecExamples::test_example_a_two_column_table."""
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

    def test_explicit_empty_field_metadata_treated_as_no_metadata(self):
        schema_none = pa.schema([pa.field("x", pa.int32())])
        schema_empty = pa.schema([pa.field("x", pa.int32(), metadata={})])
        assert (
            ArrowDigester.hash_schema(schema_none, include_metadata=True)
            == ArrowDigester.hash_schema(schema_empty, include_metadata=True)
        )


class TestRoundTrip:
    def test_add_then_change_metadata_both_differ(self):
        """Adding and then changing metadata each produce distinct hashes."""
        schema_plain = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        schema_v1 = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"version": b"1"}),
        ])
        schema_v2 = pa.schema([
            pa.field("x", pa.int32(), nullable=False, metadata={b"version": b"2"}),
        ])
        h_plain = ArrowDigester.hash_schema(schema_plain, include_metadata=True)
        h_v1 = ArrowDigester.hash_schema(schema_v1, include_metadata=True)
        h_v2 = ArrowDigester.hash_schema(schema_v2, include_metadata=True)
        assert h_plain != h_v1
        assert h_v1 != h_v2
        assert h_plain != h_v2

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

    def test_nested_struct_child_metadata_round_trip(self):
        """Round-trip: add/remove metadata on a nested struct child field."""
        child_plain = pa.field("age", pa.int32(), nullable=False)
        child_meta = pa.field("age", pa.int32(), nullable=False, metadata={b"unit": b"years"})
        schema_plain = pa.schema([pa.field("person", pa.struct([child_plain]))])
        schema_meta = pa.schema([pa.field("person", pa.struct([child_meta]))])

        h_plain = ArrowDigester.hash_schema(schema_plain, include_metadata=True)
        h_meta = ArrowDigester.hash_schema(schema_meta, include_metadata=True)
        assert h_plain != h_meta

        # Removing the child metadata restores the original hash
        schema_restored = pa.schema([pa.field("person", pa.struct([child_plain]))])
        assert ArrowDigester.hash_schema(schema_restored, include_metadata=True) == h_plain

    def test_list_element_field_metadata_round_trip(self):
        """Round-trip: add/remove metadata on a list element field (trailing-slash path)."""
        element_plain = pa.field("item", pa.int32(), nullable=False)
        element_meta = pa.field("item", pa.int32(), nullable=False, metadata={b"unit": b"count"})
        schema_plain = pa.schema([pa.field("items", pa.large_list(element_plain))])
        schema_meta = pa.schema([pa.field("items", pa.large_list(element_meta))])

        h_plain = ArrowDigester.hash_schema(schema_plain, include_metadata=True)
        h_meta = ArrowDigester.hash_schema(schema_meta, include_metadata=True)
        assert h_plain != h_meta

        # Removing the element field metadata restores the original hash
        schema_restored = pa.schema([pa.field("items", pa.large_list(element_plain))])
        assert ArrowDigester.hash_schema(schema_restored, include_metadata=True) == h_plain


class TestFieldPathSorting:
    def test_fields_are_globally_sorted_by_path(self):
        """Field paths are sorted alphabetically regardless of schema field definition order."""
        schema_za = pa.schema([
            pa.field("z_col", pa.int32(), metadata={b"k": b"1"}),
            pa.field("a_col", pa.int32(), metadata={b"k": b"2"}),
        ])
        schema_az = pa.schema([
            pa.field("a_col", pa.int32(), metadata={b"k": b"2"}),
            pa.field("z_col", pa.int32(), metadata={b"k": b"1"}),
        ])
        assert (
            ArrowDigester.hash_schema(schema_za, include_metadata=True)
            == ArrowDigester.hash_schema(schema_az, include_metadata=True)
        )
