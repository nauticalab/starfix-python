"""Tests for include_metadata hashing option (PLT-1734).

Verifies that:
- include_metadata=False (default) ignores all Arrow metadata
- include_metadata=True detects changes to schema-level and field-level metadata
- Hashes are deterministic regardless of metadata key insertion order
- Empty-metadata invariant: no metadata → same hash regardless of include_metadata
"""
from __future__ import annotations

import pyarrow as pa
import pytest
from starfix.arrow_digester import ArrowDigester


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

    def test_default_matches_pinned_golden_value(self):
        """Batch with metadata + include_metadata=False must match the pinned
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
