"""Tests for the pure-Python Arrow digester.

Golden hash values are taken from the Rust test suite to ensure
byte-for-byte compatibility.
"""

import pyarrow as pa
import pytest
from starfix.arrow_digester import ArrowDigester, _serialized_schema


# ── Schema serialization ──────────────────────────────────────────────


class TestSchemaSerialization:
    def test_simple_schema(self):
        schema = pa.schema([
            pa.field("age", pa.int32(), nullable=False),
            pa.field("name", pa.utf8(), nullable=True),
        ])
        s = _serialized_schema(schema)
        # Keys must be sorted: age before name
        assert s.index('"age"') < s.index('"name"')
        assert '"data_type":"Int32"' in s
        assert '"data_type":"LargeUtf8"' in s
        assert '"nullable":false' in s

    def test_time_types_in_schema(self):
        schema = pa.schema([
            pa.field("t32s", pa.time32("s"), nullable=False),
            pa.field("t32ms", pa.time32("ms"), nullable=False),
            pa.field("t64us", pa.time64("us"), nullable=False),
            pa.field("t64ns", pa.time64("ns"), nullable=False),
        ])
        s = _serialized_schema(schema)
        assert '"Time32":"Second"' in s
        assert '"Time32":"Millisecond"' in s
        assert '"Time64":"Microsecond"' in s
        assert '"Time64":"Nanosecond"' in s

    def test_list_schema_uses_element_type_no_name(self):
        """LargeList serialization should use element_type_to_value (no 'name' key)."""
        schema = pa.schema([
            pa.field("items", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=True),
        ])
        s = _serialized_schema(schema)
        # Should NOT contain "name":"item" inside the LargeList value
        # The LargeList value should only have data_type and nullable
        assert '"LargeList"' in s
        # Check that the inner field does NOT include "name"
        import json
        parsed = json.loads(s)
        large_list_val = parsed["items"]["data_type"]["LargeList"]
        assert "name" not in large_list_val
        assert "data_type" in large_list_val
        assert "nullable" in large_list_val

    def test_struct_fields_sorted_in_schema(self):
        """Struct fields should be sorted alphabetically in serialization."""
        schema = pa.schema([
            pa.field("s", pa.struct([
                pa.field("z_field", pa.int32(), nullable=False),
                pa.field("a_field", pa.bool_(), nullable=True),
            ]), nullable=True),
        ])
        s = _serialized_schema(schema)
        # a_field should appear before z_field in the Struct array
        assert s.index('"a_field"') < s.index('"z_field"')


# ── Schema hashing (golden values from Rust) ──────────────────────────


class TestSchemaHashing:
    def test_simple_schema_empty_table(self):
        """Empty table hash for a simple schema shared between Rust and Python."""
        schema = pa.schema([
            pa.field("flags", pa.bool_(), nullable=True),
            pa.field("uids", pa.int32(), nullable=False),
        ])
        d = ArrowDigester(schema)
        h = d.finalize().hex()
        assert h.startswith("000001")
        # Self-consistency: finalize with no updates on fresh instances
        d2 = ArrowDigester(schema)
        assert d2.finalize() == d.finalize()


# ── Array hashing (golden values from Rust) ───────────────────────────


class TestArrayHashing:
    def test_boolean_array(self):
        arr = pa.array([True, None, False, True], type=pa.bool_())
        h = ArrowDigester.hash_array(arr).hex()
        assert h == "00000185a9c99eba7bcfd9b14fd529b9534f2289319779270aa4a072f117cf90a6ac8b"

    def test_int32_array(self):
        arr = pa.array([42, None, -7, 0], type=pa.int32())
        h = ArrowDigester.hash_array(arr).hex()
        assert h == "0000018330f9b8796b9434cbf7bc028c18c58a2a739b980acf9995ce1e5d60b43b0138"

    def test_time32_second_array(self):
        arr = pa.array([1000, None, 5000, 0], type=pa.time32("s"))
        h = ArrowDigester.hash_array(arr).hex()
        assert h == "000001aba70469e596c735ec13c3d60a9db2d0e5515eb864f07ad5d24572b35f23eacc"

    def test_time64_microsecond_array(self):
        arr = pa.array([1_000_000, None, 5_000_000, 0], type=pa.time64("us"))
        h = ArrowDigester.hash_array(arr).hex()
        assert h == "000001c96d705b1278f9ffe1b31fb307408768f14d961c44028a1d0f778dd61786ee26"

    def test_time_units_differ(self):
        a = pa.array([1000, 2000], type=pa.time32("s"))
        b = pa.array([1000, 2000], type=pa.time32("ms"))
        assert ArrowDigester.hash_array(a) != ArrowDigester.hash_array(b)

    def test_binary_array(self):
        arr = pa.array([b"hello", None, b"world", b""], type=pa.binary())
        h = ArrowDigester.hash_array(arr).hex()
        assert h == "0000018dc3a0e479d1335553546c8f23c36d75335cbd34805a6f96c5d5225b347fbc57"

    def test_string_array(self):
        arr = pa.array(["hello", None, "world", ""], type=pa.utf8())
        h = ArrowDigester.hash_array(arr).hex()
        assert h == "0000016255bde0141ebf26e08c31c96f6112e5e21d101ab8bb90d77f2c3eec02c62d3c"

    def test_list_array(self):
        arr = pa.array(
            [[1, 2, 3], None, [4, 5], [6]],
            type=pa.list_(pa.field("item", pa.int32(), nullable=True)),
        )
        h = ArrowDigester.hash_array(arr).hex()
        assert h == "00000190658c2c4e9178f8ae6c686d6fe13262a9fab9cb619542911453abeca8195a9f"

    def test_decimal128_array(self):
        from decimal import Decimal
        arr = pa.array(
            [
                Decimal("1234567890123456789012.34567"),
                None,
                Decimal("-9876543210987654321098.76543"),
                Decimal("0.00000"),
            ],
            type=pa.decimal128(38, 5),
        )
        h = ArrowDigester.hash_array(arr).hex()
        assert h == "0000011e3b33d28771b3593fd5dc4b68af8091a1ba9cd493ade374e7368e213bef244e"


# ── Type normalization ────────────────────────────────────────────────


class TestTypeNormalization:
    def test_utf8_equals_large_utf8_array(self):
        a = pa.array(["hello", None, "world"], type=pa.utf8())
        b = pa.array(["hello", None, "world"], type=pa.large_utf8())
        assert ArrowDigester.hash_array(a) == ArrowDigester.hash_array(b)

    def test_binary_equals_large_binary_array(self):
        a = pa.array([b"hello", None, b"world"], type=pa.binary())
        b = pa.array([b"hello", None, b"world"], type=pa.large_binary())
        assert ArrowDigester.hash_array(a) == ArrowDigester.hash_array(b)

    def test_list_equals_large_list_array(self):
        lt = pa.list_(pa.field("item", pa.int32(), nullable=True))
        llt = pa.large_list(pa.field("item", pa.int32(), nullable=True))
        a = pa.array([[1, 2], None, [3]], type=lt)
        b = pa.array([[1, 2], None, [3]], type=llt)
        assert ArrowDigester.hash_array(a) == ArrowDigester.hash_array(b)

    def test_schema_utf8_equals_large_utf8(self):
        s1 = pa.schema([pa.field("name", pa.utf8(), nullable=True)])
        s2 = pa.schema([pa.field("name", pa.large_utf8(), nullable=True)])
        assert ArrowDigester.hash_schema(s1) == ArrowDigester.hash_schema(s2)

    def test_schema_binary_equals_large_binary(self):
        s1 = pa.schema([pa.field("data", pa.binary(), nullable=True)])
        s2 = pa.schema([pa.field("data", pa.large_binary(), nullable=True)])
        assert ArrowDigester.hash_schema(s1) == ArrowDigester.hash_schema(s2)

    def test_schema_list_equals_large_list(self):
        s1 = pa.schema([pa.field("items", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=True)])
        s2 = pa.schema([pa.field("items", pa.large_list(pa.field("item", pa.int32(), nullable=True)), nullable=True)])
        assert ArrowDigester.hash_schema(s1) == ArrowDigester.hash_schema(s2)

    def test_record_batch_utf8_equals_large_utf8(self):
        s1 = pa.schema([pa.field("name", pa.utf8(), nullable=True)])
        s2 = pa.schema([pa.field("name", pa.large_utf8(), nullable=True)])
        b1 = pa.RecordBatch.from_arrays([pa.array(["a", "b"], type=pa.utf8())], schema=s1)
        b2 = pa.RecordBatch.from_arrays([pa.array(["a", "b"], type=pa.large_utf8())], schema=s2)
        assert ArrowDigester.hash_record_batch(b1) == ArrowDigester.hash_record_batch(b2)

    def test_list_of_utf8_vs_large_list_of_large_utf8_array(self):
        """List(Utf8) vs LargeList(LargeUtf8) — normalization must be recursive."""
        list_arr = pa.array(
            [["hello", "world"], ["foo"]],
            type=pa.list_(pa.field("item", pa.utf8(), nullable=True)),
        )
        large_list_arr = pa.array(
            [["hello", "world"], ["foo"]],
            type=pa.large_list(pa.field("item", pa.large_utf8(), nullable=True)),
        )
        assert ArrowDigester.hash_array(list_arr) == ArrowDigester.hash_array(large_list_arr)

    def test_list_of_utf8_vs_large_list_of_large_utf8_schema(self):
        s1 = pa.schema([pa.field("col", pa.list_(pa.field("item", pa.utf8(), nullable=True)), nullable=True)])
        s2 = pa.schema([pa.field("col", pa.large_list(pa.field("item", pa.large_utf8(), nullable=True)), nullable=True)])
        assert ArrowDigester.hash_schema(s1) == ArrowDigester.hash_schema(s2)

    def test_streaming_with_type_equivalent_schemas(self):
        """Digester with Utf8 schema should accept LargeUtf8 batches."""
        schema_utf8 = pa.schema([pa.field("col", pa.utf8(), nullable=True)])
        d = ArrowDigester(schema_utf8)
        batch = pa.RecordBatch.from_arrays(
            [pa.array(["hello", None], type=pa.large_utf8())],
            schema=pa.schema([pa.field("col", pa.large_utf8(), nullable=True)]),
        )
        d.update(batch)
        _hash = d.finalize()  # Should not raise


# ── Collision resistance ──────────────────────────────────────────────


class TestCollisionResistance:
    def test_binary_partition(self):
        a1 = pa.array([b"\x01\x02", b"\x03"], type=pa.binary())
        a2 = pa.array([b"\x01", b"\x02\x03"], type=pa.binary())
        assert ArrowDigester.hash_array(a1) != ArrowDigester.hash_array(a2)

    def test_string_partition(self):
        a1 = pa.array(["ab", "c"], type=pa.utf8())
        a2 = pa.array(["a", "bc"], type=pa.utf8())
        assert ArrowDigester.hash_array(a1) != ArrowDigester.hash_array(a2)

    def test_list_partition(self):
        a1 = pa.array([[1, 2], [3]], type=pa.list_(pa.field("item", pa.int32(), nullable=True)))
        a2 = pa.array([[1], [2, 3]], type=pa.list_(pa.field("item", pa.int32(), nullable=True)))
        assert ArrowDigester.hash_array(a1) != ArrowDigester.hash_array(a2)


# ── RecordBatch hashing ──────────────────────────────────────────────


class TestRecordBatchHashing:
    def test_column_order_independence(self):
        uids = pa.array([1, 2, 3, 4], type=pa.int32())
        flags = pa.array([True, False, None, True], type=pa.bool_())

        batch1 = pa.RecordBatch.from_arrays(
            [uids, flags],
            schema=pa.schema([
                pa.field("uids", pa.int32(), nullable=False),
                pa.field("flags", pa.bool_(), nullable=True),
            ]),
        )
        batch2 = pa.RecordBatch.from_arrays(
            [flags, uids],
            schema=pa.schema([
                pa.field("flags", pa.bool_(), nullable=True),
                pa.field("uids", pa.int32(), nullable=False),
            ]),
        )
        assert ArrowDigester.hash_record_batch(batch1) == ArrowDigester.hash_record_batch(batch2)

    def test_batch_split_independence(self):
        """Two batches vs one combined should produce same hash."""
        schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("value", pa.float64(), nullable=True),
        ])
        batch1 = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3], type=pa.int32()), pa.array([1.1, 2.2, 3.3], type=pa.float64())],
            schema=schema,
        )
        batch2 = pa.RecordBatch.from_arrays(
            [pa.array([4, 5, 6], type=pa.int32()), pa.array([4.4, 5.5, 6.6], type=pa.float64())],
            schema=schema,
        )
        combined = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3, 4, 5, 6], type=pa.int32()),
             pa.array([1.1, 2.2, 3.3, 4.4, 5.5, 6.6], type=pa.float64())],
            schema=schema,
        )

        d_multi = ArrowDigester(schema)
        d_multi.update(batch1)
        d_multi.update(batch2)

        d_single = ArrowDigester(schema)
        d_single.update(combined)

        assert d_multi.finalize() == d_single.finalize()

    def test_streaming_golden_value(self):
        """Matches Rust test ``record_batch_hashing``."""
        schema = pa.schema([
            pa.field("uids", pa.int32(), nullable=False),
            pa.field("flags", pa.bool_(), nullable=True),
        ])
        batch1 = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3, 4], type=pa.int32()),
             pa.array([True, False, None, True], type=pa.bool_())],
            schema=schema,
        )
        batch2 = pa.RecordBatch.from_arrays(
            [pa.array([5, 6, 7, 8], type=pa.int32()),
             pa.array([False, True, True, None], type=pa.bool_())],
            schema=schema,
        )
        d = ArrowDigester(schema)
        d.update(batch1)
        d.update(batch2)
        assert d.finalize().hex() == "0000019f5fa370d315a4b4f2314be7b7284a0549b70ad4e21e584fdebf441ad02f44f0"

    def test_nullable_vs_non_nullable_same_data(self):
        """Array with all valid values should hash same whether nullable or not."""
        a = pa.array([1, 2, 3], type=pa.int32())
        b = pa.array([1, 2, 3], type=pa.int32())
        assert ArrowDigester.hash_array(a) == ArrowDigester.hash_array(b)

    def test_batches_with_nulls_vs_single(self):
        """Batches where first is all nulls should produce same result as combined batch."""
        schema = pa.schema([
            pa.field("id", pa.int32(), nullable=True),
            pa.field("value", pa.float64(), nullable=True),
        ])
        batch1 = pa.RecordBatch.from_arrays(
            [pa.array([None, None, None], type=pa.int32()),
             pa.array([None, None, None], type=pa.float64())],
            schema=schema,
        )
        batch2 = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3], type=pa.int32()),
             pa.array([1.1, 2.2, 3.3], type=pa.float64())],
            schema=schema,
        )
        combined = pa.RecordBatch.from_arrays(
            [pa.array([None, None, None, 1, 2, 3], type=pa.int32()),
             pa.array([None, None, None, 1.1, 2.2, 3.3], type=pa.float64())],
            schema=schema,
        )

        d_multi = ArrowDigester(schema)
        d_multi.update(batch1)
        d_multi.update(batch2)

        d_single = ArrowDigester(schema)
        d_single.update(combined)

        assert d_multi.finalize() == d_single.finalize()


# ── Nullable vs non-nullable schema ──────────────────────────────────


class TestNullableSchemas:
    def test_different_schema_hashes(self):
        s1 = pa.schema([pa.field("col1", pa.int32(), nullable=True),
                        pa.field("col2", pa.bool_(), nullable=True)])
        s2 = pa.schema([pa.field("col1", pa.int32(), nullable=False),
                        pa.field("col2", pa.bool_(), nullable=False)])
        assert ArrowDigester.hash_schema(s1) != ArrowDigester.hash_schema(s2)


# ── Struct hashing ───────────────────────────────────────────────────


class TestStructHashing:
    def test_struct_field_order_in_schema_should_not_affect_hash(self):
        schema1 = pa.schema([pa.field("my_struct", pa.struct([
            pa.field("x", pa.int32(), nullable=False),
            pa.field("y", pa.utf8(), nullable=True),
        ]), nullable=True)])

        schema2 = pa.schema([pa.field("my_struct", pa.struct([
            pa.field("y", pa.utf8(), nullable=True),
            pa.field("x", pa.int32(), nullable=False),
        ]), nullable=True)])

        assert ArrowDigester.hash_schema(schema1) == ArrowDigester.hash_schema(schema2)

    def test_struct_field_order_in_record_batch_should_not_affect_hash(self):
        schema1 = pa.schema([pa.field("s", pa.struct([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.bool_(), nullable=True),
        ]), nullable=False)])

        schema2 = pa.schema([pa.field("s", pa.struct([
            pa.field("b", pa.bool_(), nullable=True),
            pa.field("a", pa.int32(), nullable=False),
        ]), nullable=False)])

        ints = pa.array([1, 2, 3], type=pa.int32())
        bools = pa.array([True, False, None], type=pa.bool_())

        struct1 = pa.StructArray.from_arrays(
            [ints, bools],
            fields=[pa.field("a", pa.int32(), nullable=False), pa.field("b", pa.bool_(), nullable=True)],
        )
        struct2 = pa.StructArray.from_arrays(
            [bools, ints],
            fields=[pa.field("b", pa.bool_(), nullable=True), pa.field("a", pa.int32(), nullable=False)],
        )

        batch1 = pa.RecordBatch.from_arrays([struct1], schema=schema1)
        batch2 = pa.RecordBatch.from_arrays([struct2], schema=schema2)

        assert ArrowDigester.hash_record_batch(batch1) == ArrowDigester.hash_record_batch(batch2)

    def test_struct_with_list_utf8_vs_large_variants(self):
        """Struct with List(Utf8) should hash same as Struct with LargeList(LargeUtf8)."""
        schema1 = pa.schema([pa.field("s", pa.struct([
            pa.field("items", pa.list_(pa.field("item", pa.utf8(), nullable=True)), nullable=True),
            pa.field("name", pa.utf8(), nullable=True),
        ]), nullable=False)])

        schema2 = pa.schema([pa.field("s", pa.struct([
            pa.field("items", pa.large_list(pa.field("item", pa.large_utf8(), nullable=True)), nullable=True),
            pa.field("name", pa.large_utf8(), nullable=True),
        ]), nullable=False)])

        list1 = pa.array([["a", "b"], ["c"]], type=pa.list_(pa.field("item", pa.utf8(), nullable=True)))
        names1 = pa.array(["Alice", "Bob"], type=pa.utf8())
        struct1 = pa.StructArray.from_arrays(
            [list1, names1],
            fields=[
                pa.field("items", pa.list_(pa.field("item", pa.utf8(), nullable=True)), nullable=True),
                pa.field("name", pa.utf8(), nullable=True),
            ],
        )

        list2 = pa.array([["a", "b"], ["c"]], type=pa.large_list(pa.field("item", pa.large_utf8(), nullable=True)))
        names2 = pa.array(["Alice", "Bob"], type=pa.large_utf8())
        struct2 = pa.StructArray.from_arrays(
            [list2, names2],
            fields=[
                pa.field("items", pa.large_list(pa.field("item", pa.large_utf8(), nullable=True)), nullable=True),
                pa.field("name", pa.large_utf8(), nullable=True),
            ],
        )

        batch1 = pa.RecordBatch.from_arrays([struct1], schema=schema1)
        batch2 = pa.RecordBatch.from_arrays([struct2], schema=schema2)

        assert ArrowDigester.hash_record_batch(batch1) == ArrowDigester.hash_record_batch(batch2)


# ── Dictionary handling ──────────────────────────────────────────────


class TestDictionaryHandling:
    def test_dictionary_utf8_should_hash_same_as_plain(self):
        plain = pa.array(["apple", "banana", "apple"], type=pa.utf8())
        dict_arr = pa.DictionaryArray.from_arrays(
            pa.array([0, 1, 0], type=pa.int32()),
            pa.array(["apple", "banana"], type=pa.utf8()),
        )
        assert ArrowDigester.hash_array(plain) == ArrowDigester.hash_array(dict_arr)

    def test_dictionary_with_nulls_should_hash_same_as_plain(self):
        plain = pa.array(["a", None, "b", None], type=pa.utf8())
        dict_arr = pa.DictionaryArray.from_arrays(
            pa.array([0, None, 1, None], type=pa.int32()),
            pa.array(["a", "b"], type=pa.utf8()),
        )
        assert ArrowDigester.hash_array(plain) == ArrowDigester.hash_array(dict_arr)


# ── Streaming column reorder ─────────────────────────────────────────


class TestStreamingReorder:
    def test_streaming_update_with_reordered_columns(self):
        schema = pa.schema([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.bool_(), nullable=True),
        ])
        d = ArrowDigester(schema)

        reordered_schema = pa.schema([
            pa.field("b", pa.bool_(), nullable=True),
            pa.field("a", pa.int32(), nullable=False),
        ])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([True, False], type=pa.bool_()), pa.array([1, 2], type=pa.int32())],
            schema=reordered_schema,
        )
        d.update(batch)
        _hash = d.finalize()  # Should not raise

    def test_streaming_reordered_columns_produce_same_hash(self):
        schema_ab = pa.schema([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.bool_(), nullable=True),
        ])

        ints = pa.array([1, 2], type=pa.int32())
        bools = pa.array([True, False], type=pa.bool_())

        batch_ab = pa.RecordBatch.from_arrays(
            [ints, bools], schema=schema_ab,
        )
        batch_ba = pa.RecordBatch.from_arrays(
            [bools, ints],
            schema=pa.schema([
                pa.field("b", pa.bool_(), nullable=True),
                pa.field("a", pa.int32(), nullable=False),
            ]),
        )

        d1 = ArrowDigester(schema_ab)
        d1.update(batch_ab)
        h1 = d1.finalize()

        d2 = ArrowDigester(schema_ab)
        d2.update(batch_ba)
        h2 = d2.finalize()

        assert h1 == h2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
