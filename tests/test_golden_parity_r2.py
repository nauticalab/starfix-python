"""Golden-hash parity tests — Round 2.

Hash format 2: every expected value is a Rust-produced format-2 value from
tests/golden/format1_to_format2.json (ITL-898); the format-1 values these tests
pinned originally map to them one to one.
The Rust values are the authoritative source of truth; these tests fail
immediately if the Python implementation diverges.

Covers 40+ distinct Arrow tables / arrays across categories:
  - Empty arrays (0 elements)
  - All-null arrays
  - Large boolean arrays (multi-byte bitvec)
  - Single-element arrays
  - Nullable structs (struct itself nullable, hash_array & record batch)
  - Three-level nested structs
  - Struct with list child
  - List<Struct>, nullable List<Struct>
  - List<Struct> with nullable vs non-nullable item field (PLT-1048 edge case)
  - Non-nullable lists
  - Mixed-type structs
  - Mixed struct+list record batches
  - Empty record batches (schema only)
  - All-null columns in batches
  - Unicode strings
  - Binary with varied sizes
  - Struct field order independence
  - Nested struct with nullable inner struct
  - List-of-list (nested lists)
  - Large schema hashes
  - Multi-batch streaming with structs and lists
  - Float special values (NaN, Inf)
  - Realistic batch (struct + list + scalar)
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from starfix.arrow_digester import ArrowDigester


# ---------------------------------------------------------------------------
# Empty arrays
# ---------------------------------------------------------------------------


class TestEmptyArrays:
    def test_empty_int32_array(self):
        """Empty Int32 array (0 elements)."""
        array = pa.array([], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000023d78a36c387f0a44f74de1ccfde1cf2d8c0fc078b4d1f47d16e5810cffd3be7f"

    def test_empty_bool_array(self):
        """Empty Boolean array (0 elements)."""
        array = pa.array([], type=pa.bool_())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000020a3aed926ea2ae4c757b1505aa0c887ff47d1af823b1acc601036a4310ede4b4"


# ---------------------------------------------------------------------------
# All-null arrays
# ---------------------------------------------------------------------------


class TestAllNullArrays:
    def test_all_null_int32(self):
        """Int32 array where every element is null."""
        array = pa.array([None, None, None, None], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002047d4c739f64e9d20e876901a967f19b17e7c3f749e30342f03f798d51e04d33"

    def test_all_null_string(self):
        """LargeString array where every element is null."""
        array = pa.array([None, None, None], type=pa.large_utf8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002879ade9f01775e3d97277fbd1ec3c48e0281c39847f96869d3a94dd3e8d355fc"


# ---------------------------------------------------------------------------
# Boolean arrays
# ---------------------------------------------------------------------------


class TestBooleanArrays:
    def test_bool_17_elements(self):
        """17-element boolean array (multi-byte bitvec)."""
        array = pa.array(
            [
                True, False, True, True,
                None, False, True, False,
                True, None, True, False,
                True, True, False, None,
                True,
            ],
            type=pa.bool_(),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002f8e53d6dd8d7337e07b15dc8dc83d0636c3a1007d936ffa058279d614c08b068"


# ---------------------------------------------------------------------------
# Single-element arrays
# ---------------------------------------------------------------------------


class TestSingleElementArrays:
    def test_single_int64(self):
        """Single Int64 value: 9_999_999_999."""
        array = pa.array([9_999_999_999], type=pa.int64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002ae4689b3dd6b1e4371f832a8df5b1aee55482f8fc995866e09adf37e3a80b770"

    def test_single_string(self):
        """Single LargeString value: 'hello world'."""
        array = pa.array(["hello world"], type=pa.large_utf8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000204f74cfa9c23214afb9bc7a19fd6815b1c87115bbd295fbd47078e1b90814648"

    def test_single_bool_false(self):
        """Single Boolean value: false."""
        array = pa.array([False], type=pa.bool_())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000240973c422a2b514e91f409638b9caa9b490e84022e4c85cc7ebc438b08603155"


# ---------------------------------------------------------------------------
# Nullable struct via hash_array
# ---------------------------------------------------------------------------


class TestNullableStructHashArray:
    def test_nullable_struct_hash_array(self):
        """Struct with null buffer via hash_array — row 1 is null struct.

        Fields: a: Int32 non-null, b: LargeUtf8 non-null
        Data: [{a:10, b:'x'}, NULL, {a:30, b:'z'}]
        """
        a_arr = pa.array([10, 20, 30], type=pa.int32())
        b_arr = pa.array(["x", "y", "z"], type=pa.large_utf8())
        mask = pa.array([False, True, False], type=pa.bool_())
        fields = [
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.large_utf8(), nullable=False),
        ]
        struct_array = pa.StructArray.from_arrays(
            [a_arr, b_arr],
            fields=fields,
            mask=mask,
        )
        result = ArrowDigester.hash_array(struct_array).hex()
        assert result == "0000021cff2f979e54cd49abdd8d43a19929b0e023ea98a78b7c45700c0d4de7fb5d21"


# ---------------------------------------------------------------------------
# Three-level nested struct
# ---------------------------------------------------------------------------


class TestThreeLevelStruct:
    def test_three_level_struct_batch(self):
        """Three-level nested struct as record batch.

        top: Struct<x: Int32, mid: Struct<y: Int32, z: Int32>>
        Data: [{x:100, mid:{y:200, z:300}}]
        """
        inner_type = pa.struct([
            pa.field("y", pa.int32(), nullable=False),
            pa.field("z", pa.int32(), nullable=False),
        ])
        outer_type = pa.struct([
            pa.field("x", pa.int32(), nullable=False),
            pa.field("mid", inner_type, nullable=False),
        ])
        schema = pa.schema([pa.field("top", outer_type, nullable=False)])
        col = pa.array(
            [{"x": 100, "mid": {"y": 200, "z": 300}}],
            type=outer_type,
        )
        batch = pa.record_batch({"top": col}, schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002009faf5f41303751117910475f04c0c6685450681ca6f45daf01446040b984db"

    def test_three_level_struct_schema(self):
        """Schema hash for three-level nested struct."""
        inner_type = pa.struct([
            pa.field("y", pa.int32(), nullable=False),
            pa.field("z", pa.int32(), nullable=False),
        ])
        outer_type = pa.struct([
            pa.field("x", pa.int32(), nullable=False),
            pa.field("mid", inner_type, nullable=False),
        ])
        schema = pa.schema([pa.field("top", outer_type, nullable=False)])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "0000024884c452f6c13da2080e1e9213a74a9dab3b12a0f710e9721d74a12881971bb8"

    def test_three_level_struct_hash_array(self):
        """Three-level struct via hash_array.

        Struct<inner: Struct<y: Int32, z: LargeUtf8>, x: Int32>
        Data: [{inner:{y:7,z:'a'}, x:5}, {inner:{y:8,z:'b'}, x:6}]
        """
        y_arr = pa.array([7, 8], type=pa.int32())
        z_arr = pa.array(["a", "b"], type=pa.large_utf8())
        inner_fields = [
            pa.field("y", pa.int32(), nullable=False),
            pa.field("z", pa.large_utf8(), nullable=False),
        ]
        inner = pa.StructArray.from_arrays(
            [y_arr, z_arr],
            fields=inner_fields,
        )
        x_arr = pa.array([5, 6], type=pa.int32())
        inner_type = pa.struct(inner_fields)
        outer_fields = [
            pa.field("inner", inner_type, nullable=False),
            pa.field("x", pa.int32(), nullable=False),
        ]
        outer = pa.StructArray.from_arrays(
            [inner, x_arr],
            fields=outer_fields,
        )
        result = ArrowDigester.hash_array(outer).hex()
        assert result == "000002c390050372191220b574a0f88980a7cdf8a7982c25c3d5c6d2352e744e200b26"


# ---------------------------------------------------------------------------
# Struct with list child
# ---------------------------------------------------------------------------


class TestStructWithListChild:
    def test_struct_with_list_child_batch(self):
        """Record batch: struct with list child.

        record: Struct<id: Int32, tags: List<Int32 nullable>>
        Data: [{id:1, tags:[10,20]}, {id:2, tags:[30]}, {id:3, tags:null}]
        """
        ids = pa.array([1, 2, 3], type=pa.int32())
        tags = pa.array(
            [[10, 20], [30], None],
            type=pa.list_(pa.field("item", pa.int32(), nullable=True)),
        )
        struct_type = pa.struct([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("tags", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=True),
        ])
        struct_arr = pa.StructArray.from_arrays(
            [ids, tags],
            names=["id", "tags"],
        )
        schema = pa.schema([pa.field("record", struct_type, nullable=False)])
        batch = pa.record_batch([struct_arr], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002a8d980079fe57b52e69f297f5614ec02e45fdb53fdabc08cf05caa92c6064a83"

    def test_struct_with_list_child_schema(self):
        """Schema hash for struct with list child."""
        struct_type = pa.struct([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("tags", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=True),
        ])
        schema = pa.schema([pa.field("record", struct_type, nullable=False)])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "0000025fcffef6ce20bf6df0f21b8943b4b11045662c09d8d0770b8fa842d662a013f2"


# ---------------------------------------------------------------------------
# Nullable struct in record batch
# ---------------------------------------------------------------------------


class TestNullableStructBatch:
    def test_nullable_struct_batch(self):
        """Nullable struct in record batch — row 1 is null.

        s: Struct<a: Int32 non-null, b: Boolean non-null> nullable
        Data: [{a:1,b:true}, NULL, {a:3,b:true}]
        """
        a_arr = pa.array([1, 2, 3], type=pa.int32())
        b_arr = pa.array([True, False, True], type=pa.bool_())
        mask = pa.array([False, True, False], type=pa.bool_())
        struct_array = pa.StructArray.from_arrays(
            [a_arr, b_arr],
            names=["a", "b"],
            mask=mask,
        )
        struct_type = pa.struct([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.bool_(), nullable=False),
        ])
        schema = pa.schema([pa.field("s", struct_type, nullable=True)])
        batch = pa.record_batch([struct_array], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "0000022f9448c97edd56a482426bafca4eccd64ccbec8640970e98f0425ffc0dc95a0b"


# ---------------------------------------------------------------------------
# List<Struct> via hash_array
# ---------------------------------------------------------------------------


class TestListOfStruct:
    def test_list_of_struct_hash_array(self):
        """LargeList<Struct<id: Int32, name: LargeUtf8>> via hash_array.

        Row 0: [{id:1, name:'a'}, {id:2, name:'b'}]
        Row 1: [{id:3, name:'c'}, {id:4, name:'d'}]
        """
        ids = pa.array([1, 2, 3, 4], type=pa.int32())
        names = pa.array(["a", "b", "c", "d"], type=pa.large_utf8())
        struct_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.large_utf8(), nullable=False),
        ]
        struct_arr = pa.StructArray.from_arrays(
            [ids, names],
            fields=struct_fields,
        )
        inner_type = pa.struct([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.large_utf8(), nullable=False),
        ])
        inner_field = pa.field("item", inner_type, nullable=False)
        offsets = pa.array([0, 2, 4], type=pa.int64())
        list_array = pa.LargeListArray.from_arrays(offsets, struct_arr, type=pa.large_list(inner_field))
        result = ArrowDigester.hash_array(list_array).hex()
        assert result == "0000028264b7ca05055fba21aca4d148535baf3012f3d651999744c16a51327d5fada2"

    def test_nullable_list_of_struct_hash_array(self):
        """Nullable LargeList<Struct> via hash_array — row 1 is null.

        Row 0: [{id:10, val:'p'}]
        Row 1: NULL
        Row 2: [{id:20, val:'q'}, {id:30, val:'r'}]
        """
        ids = pa.array([10, 20, 30], type=pa.int32())
        vals = pa.array(["p", "q", "r"], type=pa.large_utf8())
        struct_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("val", pa.large_utf8(), nullable=False),
        ]
        struct_arr = pa.StructArray.from_arrays(
            [ids, vals],
            fields=struct_fields,
        )
        inner_type = pa.struct(struct_fields)
        inner_field = pa.field("item", inner_type, nullable=False)
        offsets = pa.array([0, 1, 1, 3], type=pa.int64())
        mask = pa.array([False, True, False], type=pa.bool_())
        list_array = pa.LargeListArray.from_arrays(offsets, struct_arr, type=pa.large_list(inner_field), mask=mask)
        result = ArrowDigester.hash_array(list_array).hex()
        assert result == "000002fa2beb54d6af22cf565ee84f090ad7b3d097a2381302adb44f6067f56e63a7ca"


# ---------------------------------------------------------------------------
# Non-nullable list
# ---------------------------------------------------------------------------


class TestNonNullableList:
    def test_list_non_nullable(self):
        """List<Int32> where all elements are present (no nulls at list or item level).

        Row 0: [1]
        Row 1: [2, 3]
        Row 2: [4, 5, 6]
        """
        array = pa.array(
            [[1], [2, 3], [4, 5, 6]],
            type=pa.list_(pa.field("item", pa.int32(), nullable=True)),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002153518ab6f2ec6777784fcf99acfd96dbf8d2941929284676875ae5d1bad1b96"


# ---------------------------------------------------------------------------
# Struct with mixed types
# ---------------------------------------------------------------------------


class TestStructMixedTypes:
    def test_struct_mixed_types_hash_array(self):
        """Struct with int, string, bool, and list children via hash_array.

        Data: [{id:1, name:'Alice', active:true, scores:[90,85]},
               {id:2, name:'Bob', active:false, scores:[70]}]
        """
        id_arr = pa.array([1, 2], type=pa.int32())
        name_arr = pa.array(["Alice", "Bob"], type=pa.large_utf8())
        active_arr = pa.array([True, False], type=pa.bool_())
        scores_arr = pa.array(
            [[90, 85], [70]],
            type=pa.list_(pa.field("item", pa.int32(), nullable=True)),
        )
        struct_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.large_utf8(), nullable=False),
            pa.field("active", pa.bool_(), nullable=False),
            pa.field("scores", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=False),
        ]
        struct_arr = pa.StructArray.from_arrays(
            [id_arr, name_arr, active_arr, scores_arr],
            fields=struct_fields,
        )
        result = ArrowDigester.hash_array(struct_arr).hex()
        assert result == "0000024e525a6930337eb2b8ce95fe8af77f0afd021b3e5513d9d4d0cddae8acc5e590"


# ---------------------------------------------------------------------------
# Mixed struct + list record batch
# ---------------------------------------------------------------------------


class TestMixedStructListBatch:
    def test_mixed_struct_list_batch(self):
        """Record batch with id: Int64, meta: Struct, tags: LargeList<LargeUtf8>.

        Row 0: {id:100, meta:{key:'k1', value:10}, tags:['t1','t2']}
        Row 1: {id:200, meta:{key:'k2', value:null}, tags:['t3']}
        """
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("meta", pa.struct([
                pa.field("key", pa.large_utf8(), nullable=False),
                pa.field("value", pa.int32(), nullable=True),
            ]), nullable=False),
            pa.field("tags", pa.large_list(
                pa.field("item", pa.large_utf8(), nullable=True)), nullable=True),
        ])

        id_arr = pa.array([100, 200], type=pa.int64())
        key_arr = pa.array(["k1", "k2"], type=pa.large_utf8())
        val_arr = pa.array([10, None], type=pa.int32())
        meta_arr = pa.StructArray.from_arrays(
            [key_arr, val_arr],
            names=["key", "value"],
        )

        tag_strings = pa.array(["t1", "t2", "t3"], type=pa.large_utf8())
        offsets = pa.array([0, 2, 3], type=pa.int64())
        tags_arr = pa.LargeListArray.from_arrays(
            offsets, tag_strings,
            type=pa.large_list(pa.field("item", pa.large_utf8(), nullable=True)),
        )

        batch = pa.record_batch([id_arr, meta_arr, tags_arr], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002fb371b308310a0003de7e0175d54abb92e3792573fb3afbd322e35b544ee87b0"

    def test_mixed_struct_list_schema(self):
        """Schema hash for mixed struct + list batch."""
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("meta", pa.struct([
                pa.field("key", pa.large_utf8(), nullable=False),
                pa.field("value", pa.int32(), nullable=True),
            ]), nullable=False),
            pa.field("tags", pa.large_list(
                pa.field("item", pa.large_utf8(), nullable=True)), nullable=True),
        ])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "000002fb461efa00640c67b76df00f79bc3428908daaba4ac1232368eb43bf96e616da"


# ---------------------------------------------------------------------------
# Empty record batch (schema with 0 rows)
# ---------------------------------------------------------------------------


class TestEmptyBatchComplexSchema:
    def test_empty_batch_complex_schema(self):
        """Empty table hash (ArrowDigester(schema).finalize()) with complex schema."""
        schema = pa.schema([
            pa.field("x", pa.int32(), nullable=False),
            pa.field("y", pa.large_utf8(), nullable=True),
            pa.field("z", pa.struct([
                pa.field("a", pa.bool_(), nullable=False),
                pa.field("b", pa.float64(), nullable=True),
            ]), nullable=False),
        ])
        digester = ArrowDigester(schema)
        result = digester.finalize().hex()
        assert result == "000002f6f2c584b8001599fb6a07e4e30663a931be6b269c552f7b98d717536fbcccfd"

    def test_empty_batch_complex_schema_hash(self):
        """Schema hash (hash_schema) for same complex schema."""
        schema = pa.schema([
            pa.field("x", pa.int32(), nullable=False),
            pa.field("y", pa.large_utf8(), nullable=True),
            pa.field("z", pa.struct([
                pa.field("a", pa.bool_(), nullable=False),
                pa.field("b", pa.float64(), nullable=True),
            ]), nullable=False),
        ])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "000002985afdc0c320fdd4641d7bb5cb3eebec474dac7c2aa5679eceba6c53e24186fe"


# ---------------------------------------------------------------------------
# Batch with all-null column
# ---------------------------------------------------------------------------


class TestBatchAllNullColumn:
    def test_batch_all_null_column(self):
        """Record batch where one column is entirely null.

        id: [1, 2, 3], value: [null, null, null]
        """
        schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("value", pa.large_utf8(), nullable=True),
        ])
        batch = pa.record_batch(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "value": pa.array([None, None, None], type=pa.large_utf8()),
            },
            schema=schema,
        )
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "0000023f845b906c1a04b464b662ae67a64229b9790930e69de6cc72d395e4ee8b4752"


# ---------------------------------------------------------------------------
# Unicode strings
# ---------------------------------------------------------------------------


class TestUnicodeStrings:
    def test_unicode_string_array(self):
        """LargeString array with unicode characters."""
        array = pa.array(
            ["hello", "\u4e16\u754c", "\U0001f30d\U0001f30e\U0001f30f", None, "caf\u00e9"],
            type=pa.large_utf8(),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002f996fdd3ff03ad6faefd5ad734dc7c06d8719ec05794db6b5acae5ff694f5486"


# ---------------------------------------------------------------------------
# Binary with varied sizes
# ---------------------------------------------------------------------------


class TestBinaryVariedSizes:
    def test_binary_varied_sizes(self):
        """Binary array with varied sizes including empty, null, and special bytes."""
        array = pa.array(
            [
                b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f",
                b"",
                None,
                b"\xff",
                b"\x00",
            ],
            type=pa.binary(),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002148db3622e92806eff4feafa117c429c052588ccc7b9120b03f02ff7ef26f6a1"


# ---------------------------------------------------------------------------
# Struct field order independence
# ---------------------------------------------------------------------------


class TestStructFieldOrderIndependence:
    def test_struct_3field_order_independence(self):
        """Three-field struct: field order should not affect hash.

        Fields: a: Int32, b: LargeUtf8, c: Boolean
        Data: [{a:1,b:'x',c:true}, {a:2,b:'y',c:false}]
        """
        a = pa.array([1, 2], type=pa.int32())
        b = pa.array(["x", "y"], type=pa.large_utf8())
        c = pa.array([True, False], type=pa.bool_())

        fa = pa.field("a", pa.int32(), nullable=False)
        fb = pa.field("b", pa.large_utf8(), nullable=False)
        fc = pa.field("c", pa.bool_(), nullable=False)

        # Order: a, b, c
        s1 = pa.StructArray.from_arrays([a, b, c], fields=[fa, fb, fc])
        # Order: c, a, b
        s2 = pa.StructArray.from_arrays([c, a, b], fields=[fc, fa, fb])

        h1 = ArrowDigester.hash_array(s1).hex()
        h2 = ArrowDigester.hash_array(s2).hex()
        assert h1 == h2, "struct field order should not affect hash_array"
        assert h1 == "000002df24f48f927ab26711c1db497b5892eae8779a146bdd9eae7784d49f01593283"


# ---------------------------------------------------------------------------
# Nested struct with nullable inner struct
# ---------------------------------------------------------------------------


class TestNestedNullableInnerStruct:
    def test_nested_nullable_inner_struct(self):
        """Outer struct with nullable inner struct — inner row 1 is null.

        Outer: Struct<a: Int32, inner: Struct<p: Int32, q: Int32> nullable>
        Data: [{a:10, inner:{p:1,q:4}}, {a:20, inner:NULL}, {a:30, inner:{p:3,q:6}}]
        """
        p = pa.array([1, 2, 3], type=pa.int32())
        q = pa.array([4, 5, 6], type=pa.int32())
        inner_mask = pa.array([False, True, False], type=pa.bool_())
        inner_fields = [
            pa.field("p", pa.int32(), nullable=False),
            pa.field("q", pa.int32(), nullable=False),
        ]
        inner = pa.StructArray.from_arrays(
            [p, q],
            fields=inner_fields,
            mask=inner_mask,
        )

        a = pa.array([10, 20, 30], type=pa.int32())
        inner_type = pa.struct(inner_fields)
        outer_fields = [
            pa.field("a", pa.int32(), nullable=False),
            pa.field("inner", inner_type, nullable=True),
        ]
        outer = pa.StructArray.from_arrays(
            [a, inner],
            fields=outer_fields,
        )
        result = ArrowDigester.hash_array(outer).hex()
        assert result == "000002e046c146668f0f34a7a9d212d17df6dfc55adad063b1e60b0256fb4b2fdec820"


# ---------------------------------------------------------------------------
# List of list (nested lists)
# ---------------------------------------------------------------------------


class TestListOfList:
    def test_list_of_list_hash_array(self):
        """LargeList<LargeList<Int32>> via hash_array.

        Row 0: [[1,2], [3]]
        Row 1: [[4,5], [6]]
        """
        inner_values = pa.array([1, 2, 3, 4, 5, 6], type=pa.int32())
        inner_offsets = pa.array([0, 2, 3, 5, 6], type=pa.int64())
        inner_field = pa.field("item", pa.int32(), nullable=False)
        inner_list = pa.LargeListArray.from_arrays(
            inner_offsets, inner_values,
            type=pa.large_list(inner_field),
        )
        outer_offsets = pa.array([0, 2, 4], type=pa.int64())
        outer_field = pa.field("item", pa.large_list(inner_field), nullable=False)
        outer_list = pa.LargeListArray.from_arrays(
            outer_offsets, inner_list,
            type=pa.large_list(outer_field),
        )
        result = ArrowDigester.hash_array(outer_list).hex()
        assert result == "00000223c18487aa7dcb5bfb673a0d69dc41ba597febefe1c241ff26ebc4679bbf31fa"

    def test_list_of_list_batch(self):
        """Record batch with LargeList<LargeList<Int32>> column."""
        inner_values = pa.array([1, 2, 3, 4, 5, 6], type=pa.int32())
        inner_offsets = pa.array([0, 2, 3, 5, 6], type=pa.int64())
        inner_field = pa.field("item", pa.int32(), nullable=False)
        inner_list = pa.LargeListArray.from_arrays(
            inner_offsets, inner_values,
            type=pa.large_list(inner_field),
        )
        outer_offsets = pa.array([0, 2, 4], type=pa.int64())
        outer_field = pa.field("item", pa.large_list(inner_field), nullable=False)
        outer_list = pa.LargeListArray.from_arrays(
            outer_offsets, inner_list,
            type=pa.large_list(outer_field),
        )
        schema = pa.schema([pa.field("col", pa.large_list(outer_field), nullable=False)])
        batch = pa.record_batch([outer_list], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002d4a91c629161ff8126db429572a4cd20c19ffa1d1f31db8c6eec0489d668216f"

    def test_list_of_list_schema(self):
        """Schema hash for list-of-list column."""
        inner_field = pa.field("item", pa.int32(), nullable=False)
        outer_field = pa.field("item", pa.large_list(inner_field), nullable=False)
        schema = pa.schema([pa.field("col", pa.large_list(outer_field), nullable=False)])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "000002021a0c07c4ba2f2c8c3185b66e0010ca100b17294188249024aee83cdcca6835"


# ---------------------------------------------------------------------------
# Large schema (12 fields)
# ---------------------------------------------------------------------------


class TestLargeSchema:
    def test_large_schema_12_fields(self):
        """Schema hash for 12-field schema with mixed types."""
        schema = pa.schema([
            pa.field("f01", pa.int8(), nullable=False),
            pa.field("f02", pa.int16(), nullable=True),
            pa.field("f03", pa.int32(), nullable=False),
            pa.field("f04", pa.int64(), nullable=True),
            pa.field("f05", pa.float32(), nullable=False),
            pa.field("f06", pa.float64(), nullable=True),
            pa.field("f07", pa.bool_(), nullable=False),
            pa.field("f08", pa.large_utf8(), nullable=True),
            pa.field("f09", pa.large_binary(), nullable=False),
            pa.field("f10", pa.date32(), nullable=True),
            pa.field("f11", pa.struct([
                pa.field("nested_a", pa.int32(), nullable=False),
                pa.field("nested_b", pa.large_utf8(), nullable=True),
            ]), nullable=False),
            pa.field("f12", pa.large_list(
                pa.field("item", pa.int32(), nullable=True)), nullable=True),
        ])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "0000021bd0ecec5be7d78beaa0f9af537a44d47b1a7fa9035d0f7fd0050a435b3d277d"


# ---------------------------------------------------------------------------
# Schema with nested list-of-struct
# ---------------------------------------------------------------------------


class TestSchemaListOfStructWithList:
    def test_schema_list_of_struct_with_list(self):
        """Schema hash for LargeList<Struct<ts, msg, tags: LargeList<LargeUtf8>>>."""
        schema = pa.schema([
            pa.field("events", pa.large_list(
                pa.field("item", pa.struct([
                    pa.field("ts", pa.int64(), nullable=False),
                    pa.field("msg", pa.large_utf8(), nullable=True),
                    pa.field("tags", pa.large_list(
                        pa.field("item", pa.large_utf8(), nullable=False)),
                        nullable=True),
                ]), nullable=False)), nullable=True),
        ])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "0000029ed92e6613925ec60e34390bae6bf243e8ceac257139e08b62fd7fdd759c85eb"


# ---------------------------------------------------------------------------
# Deeply nested schema (4 levels)
# ---------------------------------------------------------------------------


class TestSchema4LevelNestedStruct:
    def test_schema_4_level_nested_struct(self):
        """Schema hash for 4-level nested struct: root.l1.l2.l3.leaf."""
        schema = pa.schema([
            pa.field("root", pa.struct([
                pa.field("l1", pa.struct([
                    pa.field("l2", pa.struct([
                        pa.field("l3", pa.struct([
                            pa.field("leaf", pa.int32(), nullable=False),
                        ]), nullable=False),
                    ]), nullable=False),
                ]), nullable=False),
            ]), nullable=False),
        ])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "0000023b39cd00c1b1edbe305fcdddaf7e954d288584bfc8e08752098b408920c81162"


# ---------------------------------------------------------------------------
# Nullable list-of-struct with nullable children
# ---------------------------------------------------------------------------


class TestNullableListStructNullableChildren:
    def test_nullable_list_struct_nullable_children_batch(self):
        """Record batch: nullable LargeList<Struct> with nullable children.

        items: LargeList<Struct<id: Int32, label: LargeUtf8 nullable>> nullable
        Row 0: [{id:1, label:'a'}, {id:2, label:null}]
        Row 1: NULL
        """
        ids = pa.array([1, 2, 3], type=pa.int32())
        labels = pa.array(["a", None, "c"], type=pa.large_utf8())
        struct_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("label", pa.large_utf8(), nullable=True),
        ]
        struct_arr = pa.StructArray.from_arrays(
            [ids, labels],
            fields=struct_fields,
        )
        inner_type = pa.struct(struct_fields)
        inner_field = pa.field("item", inner_type, nullable=False)
        offsets = pa.array([0, 2, 3], type=pa.int64())
        mask = pa.array([False, True], type=pa.bool_())
        list_array = pa.LargeListArray.from_arrays(
            offsets, struct_arr,
            type=pa.large_list(inner_field),
            mask=mask,
        )
        schema = pa.schema([
            pa.field("items", pa.large_list(inner_field), nullable=True),
        ])
        batch = pa.record_batch([list_array], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002b5fde83bc94e02aa663fe4c1aad59a5280c6f0d487decd0b66801abe35d43104"


# ---------------------------------------------------------------------------
# Struct empty batch (schema only, struct fields)
# ---------------------------------------------------------------------------


class TestStructEmptyBatch:
    def test_struct_empty_batch(self):
        """Empty table hash with struct schema (0 rows)."""
        schema = pa.schema([
            pa.field("person", pa.struct([
                pa.field("name", pa.large_utf8(), nullable=False),
                pa.field("age", pa.int32(), nullable=False),
                pa.field("score", pa.float64(), nullable=False),
            ]), nullable=False),
        ])
        d = ArrowDigester(schema)
        result = d.finalize().hex()
        assert result == "0000028a33a2eeedd0ab0c78292441615250f4e2dd5d37da6d1bd98ed43ee003ea1f93"


# ---------------------------------------------------------------------------
# Multi-batch with struct column
# ---------------------------------------------------------------------------


class TestMultiBatchStruct:
    def test_multi_batch_struct(self):
        """Multi-batch streaming with struct column.

        Schema: s: Struct<x: Int32, y: Int32 nullable>
        Batch 1: [{x:1,y:10}, {x:2,y:null}]
        Batch 2: [{x:3,y:30}]
        """
        struct_type = pa.struct([
            pa.field("x", pa.int32(), nullable=False),
            pa.field("y", pa.int32(), nullable=True),
        ])
        schema = pa.schema([pa.field("s", struct_type, nullable=False)])

        s1 = pa.StructArray.from_arrays(
            [pa.array([1, 2], type=pa.int32()), pa.array([10, None], type=pa.int32())],
            names=["x", "y"],
        )
        batch1 = pa.record_batch([s1], schema=schema)

        s2 = pa.StructArray.from_arrays(
            [pa.array([3], type=pa.int32()), pa.array([30], type=pa.int32())],
            names=["x", "y"],
        )
        batch2 = pa.record_batch([s2], schema=schema)

        d = ArrowDigester(schema)
        d.update(batch1)
        d.update(batch2)
        result = d.finalize().hex()
        assert result == "0000023aa02f66b4e83360ebc47d3c74f6dd9c3ed9fde617a8218bfaaa48817f3f2da2"


# ---------------------------------------------------------------------------
# Multi-batch with list column
# ---------------------------------------------------------------------------


class TestMultiBatchList:
    def test_multi_batch_list(self):
        """Multi-batch streaming with LargeList<Int32> column.

        Batch 1: [[1,2], null]
        Batch 2: [[3,4,5]]
        """
        list_type = pa.large_list(pa.field("item", pa.int32(), nullable=True))
        schema = pa.schema([pa.field("items", list_type, nullable=True)])

        arr1 = pa.array(
            [[1, 2], None],
            type=list_type,
        )
        batch1 = pa.record_batch([arr1], schema=schema)

        arr2 = pa.array(
            [[3, 4, 5]],
            type=list_type,
        )
        batch2 = pa.record_batch([arr2], schema=schema)

        d = ArrowDigester(schema)
        d.update(batch1)
        d.update(batch2)
        result = d.finalize().hex()
        assert result == "000002a13b117b286d47ac47a1f53af3dfacb479530fb59152da40b8213429ec310519"


# ---------------------------------------------------------------------------
# LargeList<Struct> with nullable vs non-nullable inner (item) field
#
# This exercises the structural-only BTreeMap entry created for List<Struct>.
# When inner_field.nullable=True the entry's BitVec is allocated but never
# populated (bit_count=0), so 8 zero bytes are prepended to the structural
# digest at finalisation.  Both Rust and Python handle this identically.
# Golden values verified via `cargo test list_struct_nullable_inner_field_parity`
# against the Rust starfix crate (PLT-1048).
# ---------------------------------------------------------------------------


class TestListStructNullableInnerField:
    """LargeList<Struct> — nullable vs non-nullable item field parity."""

    _ids = [1, 2, 3, 4]
    _labels = ["a", "b", "c", "d"]

    def _make_list_array(self, inner_nullable: bool) -> "pa.LargeListArray":
        ids = pa.array(self._ids, type=pa.int32())
        labels = pa.array(self._labels, type=pa.large_utf8())
        struct_fields = [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("label", pa.large_utf8(), nullable=False),
        ]
        struct_arr = pa.StructArray.from_arrays(
            [ids, labels],
            fields=struct_fields,
        )
        inner_field = pa.field("item", pa.struct(struct_fields), nullable=inner_nullable)
        offsets = pa.array([0, 2, 4], type=pa.int64())
        return pa.LargeListArray.from_arrays(
            offsets,
            struct_arr,
            type=pa.large_list(inner_field),
        )

    def test_nullable_inner_field(self):
        """LargeList<Struct> with nullable=True item field.

        ids=[1,2,3,4], labels=['a','b','c','d'], offsets=[0,2,4]
        inner_field.nullable=True → structural-only entry has empty BitVec.
        Golden value confirmed against Rust (PLT-1048).
        """
        result = ArrowDigester.hash_array(self._make_list_array(inner_nullable=True)).hex()
        assert result == "000002880d5a883711e8de86c09cd3bdc4a2da0390e9ae93947c5de4d42675f1033f5a"

    def test_non_nullable_inner_field(self):
        """LargeList<Struct> with nullable=False item field.

        ids=[1,2,3,4], labels=['a','b','c','d'], offsets=[0,2,4]
        inner_field.nullable=False → no BitVec in structural-only entry.
        Golden value confirmed against Rust (PLT-1048).
        """
        result = ArrowDigester.hash_array(self._make_list_array(inner_nullable=False)).hex()
        assert result == "000002d7423b057587e976fe344330d7b29694dd4a709d66cf4217ed8d8e1ccc69a39f"

    def test_nullable_differs_from_non_nullable(self):
        """Nullable vs non-nullable inner field must produce different hashes."""
        hash_nullable = ArrowDigester.hash_array(self._make_list_array(inner_nullable=True)).hex()
        hash_not_nullable = ArrowDigester.hash_array(
            self._make_list_array(inner_nullable=False)
        ).hex()
        assert hash_nullable != hash_not_nullable, (
            "nullable and non-nullable item fields must produce distinct hashes"
        )


# ---------------------------------------------------------------------------
# Float special values
# ---------------------------------------------------------------------------


class TestFloatSpecialValues:
    def test_float64_special_values(self):
        """Float64 array with NaN, Inf, -Inf, 0.0, -0.0."""
        array = pa.array(
            [float("nan"), float("inf"), float("-inf"), 0.0, -0.0],
            type=pa.float64(),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000027660de0bf00761a94c7aa2c6550cce417d9c14e5476ae787e24d555236a973bc"


# ---------------------------------------------------------------------------
# Realistic batch (struct + list + scalar)
# ---------------------------------------------------------------------------


class TestRealisticBatch:
    def test_realistic_batch(self):
        """Realistic record batch: name, coords struct, readings list.

        name: ['A', null, 'C']
        coords: [{lat:40.7,lon:-74.0}, {lat:34.0,lon:-118.2}, {lat:51.5,lon:-0.1}]
        readings: [[1.0,2.0], null, [3.0,4.0,5.0]]
        """
        schema = pa.schema([
            pa.field("name", pa.large_utf8(), nullable=True),
            pa.field("coords", pa.struct([
                pa.field("lat", pa.float64(), nullable=False),
                pa.field("lon", pa.float64(), nullable=False),
            ]), nullable=False),
            pa.field("readings", pa.large_list(
                pa.field("item", pa.float32(), nullable=False)), nullable=True),
        ])

        name_arr = pa.array(["A", None, "C"], type=pa.large_utf8())
        lat_arr = pa.array([40.7, 34.0, 51.5], type=pa.float64())
        lon_arr = pa.array([-74.0, -118.2, -0.1], type=pa.float64())
        coords_arr = pa.StructArray.from_arrays(
            [lat_arr, lon_arr],
            names=["lat", "lon"],
        )

        reading_vals = pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float32())
        offsets = pa.array([0, 2, 2, 5], type=pa.int64())
        reading_mask = pa.array([False, True, False], type=pa.bool_())
        readings_arr = pa.LargeListArray.from_arrays(
            offsets, reading_vals,
            type=pa.large_list(pa.field("item", pa.float32(), nullable=False)),
            mask=reading_mask,
        )

        batch = pa.record_batch([name_arr, coords_arr, readings_arr], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002d334ea76f2a30807791177def39034f19caf4e6bd142c23ec05cc192298893de"

    def test_realistic_schema(self):
        """Schema hash for realistic batch."""
        schema = pa.schema([
            pa.field("name", pa.large_utf8(), nullable=True),
            pa.field("coords", pa.struct([
                pa.field("lat", pa.float64(), nullable=False),
                pa.field("lon", pa.float64(), nullable=False),
            ]), nullable=False),
            pa.field("readings", pa.large_list(
                pa.field("item", pa.float32(), nullable=False)), nullable=True),
        ])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "00000254036eae7a3edfdcd5960c95be8600f4db97f52b8100c47c4a92d79f8b444792"
