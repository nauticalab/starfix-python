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


def _with_type(arr: pa.Array, t: pa.DataType) -> pa.Array:
    """`arr` rebuilt with the field nullability of struct type `t` (same data).

    pyarrow 19 cannot cast a struct whose children are nullable to one whose children
    are not, which pa.record_batch(..., schema=...) would otherwise do.
    """
    if not pa.types.is_struct(t) or not pa.types.is_struct(arr.type):
        return arr
    children = [_with_type(arr.field(i), t.field(i).type) for i in range(t.num_fields)]
    mask = arr.is_null() if arr.null_count else None
    return pa.StructArray.from_arrays(children, fields=list(t), mask=mask)


def _record_batch(columns, schema: pa.Schema) -> pa.RecordBatch:
    if isinstance(columns, dict):
        columns = [columns[f.name] for f in schema]
    return pa.record_batch(
        [_with_type(c, f.type) if isinstance(c, pa.Array) else c for c, f in zip(columns, schema)],
        schema=schema,
    )


# ---------------------------------------------------------------------------
# Empty arrays
# ---------------------------------------------------------------------------


class TestEmptyArrays:
    def test_empty_int32_array(self):
        """Empty Int32 array (0 elements)."""
        array = pa.array([], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002b87ccbba66f376281848a40936e3b3697d877c78ec651aa4151939d9f3af2277"

    def test_empty_bool_array(self):
        """Empty Boolean array (0 elements)."""
        array = pa.array([], type=pa.bool_())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002754d4bf287e5b460d403a8468e526e92a738921b2aa8dbab8cc8a3450bcac203"


# ---------------------------------------------------------------------------
# All-null arrays
# ---------------------------------------------------------------------------


class TestAllNullArrays:
    def test_all_null_int32(self):
        """Int32 array where every element is null."""
        array = pa.array([None, None, None, None], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002a51121a9cb745bbcd08ebba07ce33ef64dcb630c241d4112d27d15106e3889d7"

    def test_all_null_string(self):
        """LargeString array where every element is null."""
        array = pa.array([None, None, None], type=pa.large_utf8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000025a88292c68392ec7d5ac54666139cb4836c9267088dfabef093e7981c322df38"


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
        assert result == "000002d8e326d69087a22ebe44e971f2673272ad044e99927c9c4cfcf1cf26f1e7dcf2"


# ---------------------------------------------------------------------------
# Single-element arrays
# ---------------------------------------------------------------------------


class TestSingleElementArrays:
    def test_single_int64(self):
        """Single Int64 value: 9_999_999_999."""
        array = pa.array([9_999_999_999], type=pa.int64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002ac6614ba2b81b9238b2888b17d9ccb5eb46a7ea3285c22a1b1ad25997d726e7b"

    def test_single_string(self):
        """Single LargeString value: 'hello world'."""
        array = pa.array(["hello world"], type=pa.large_utf8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000229b25b121cf4b425c068c2ac17b1952497cf7015668b73a62f925a078fa69d67"

    def test_single_bool_false(self):
        """Single Boolean value: false."""
        array = pa.array([False], type=pa.bool_())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000023753cbe1173b2b0f1b81ce9de03a7c0a1ba1f5431dabbdbeef99172dd357bb29"


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
        assert result == "000002f5c5e7555a574364d953e7ebeb45dbc9f232e5707133dd088420924300ae3c58"


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
        batch = _record_batch({"top": col}, schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002207e061a060534d98724ad2beabd1cd5d1350e5614104815a6a6c0ef13d8935c"

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
        assert result == "000002613ec04c0f0d49d1eff685ac3ffc3ee80a47ddb2121860f9845ac0d3a33fe7b2"

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
        assert result == "000002db3095dc0093c38b3aa668aee0f1805fc815542685d5d065008dc621512a8096"


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
        batch = _record_batch([struct_arr], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "0000027699b386e4e56ce34d2a38f748566a75c36c62ca8f5fa36180f0ea8809ac528c"

    def test_struct_with_list_child_schema(self):
        """Schema hash for struct with list child."""
        struct_type = pa.struct([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("tags", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=True),
        ])
        schema = pa.schema([pa.field("record", struct_type, nullable=False)])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "00000231878b42f48260f2ba82a63298760afe8d214504d3b97a7019ff9a83eba48682"


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
        batch = _record_batch([struct_array], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "00000201b1aee1103d35dad74cda3c5a1c01e1aa5468beb02cc3cda5c1cb6c317f2d90"


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
        assert result == "000002c1c713c7ee5c139a8799e83aff16049b6a6a935e3e3f8601e04db05c523e2d7b"

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
        assert result == "000002cd3cde5a0df63e3a6889556920e248890a882572450350a03da90f67f37e9f90"


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
        assert result == "000002c60a39367e08f911e8d99db430267db89ca41e4cef6e87ac01520422354684a6"


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
        assert result == "00000269c3b3a491ea942d3067cfdf07b67a35bd708e4ee05c45cc302190016ab4a9f7"


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

        batch = _record_batch([id_arr, meta_arr, tags_arr], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "00000227eb7540623891a51b93eb1f133f461cd3231bc03f3d5f36957b452a477b912c"

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
        assert result == "0000029f1118411cb35c292b7950a626469bd8cb987faa8c114d6509f09f32ec816cb8"


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
        assert result == "000002a157d896c73e72632c68cabed99b8351ca116199217e209552f9bfaf70721cff"

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
        assert result == "00000286ebe427c00dea251c527cc286dc591efea4d6787cb3b40d3330a476b74f6d6c"


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
        batch = _record_batch(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "value": pa.array([None, None, None], type=pa.large_utf8()),
            },
            schema=schema,
        )
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002dca3ee7298f6495f2263e45eb2c876c9dc78f5b7462357f3be5f5d6579214e94"


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
        assert result == "0000029f656d188a338cc277592a06b5710796016037efc7fc7cb5746e65bfadd752b4"


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
        assert result == "0000020885790654a7ce21e6afdfeaa4ad99490e1871149892eda8acf4d2a403566410"


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
        assert h1 == "0000025560faf3d3f3ce308ad2c7129f24f42589e7b25f0c6dc70ddd2aa45f7464111a"


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
        assert result == "00000236f16a0a8179aefeb33f440a83a7d32aad8993c022796eab539de2e1d63521b8"


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
        assert result == "000002215978d8171df62a17e8b85524418ac182dc2b1f2132ab95b8a99922dfc9c725"

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
        batch = _record_batch([outer_list], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "0000024354ac4eb6cef06e975af9b4a9f110e6baf409446897e122849553b159e6dc27"

    def test_list_of_list_schema(self):
        """Schema hash for list-of-list column."""
        inner_field = pa.field("item", pa.int32(), nullable=False)
        outer_field = pa.field("item", pa.large_list(inner_field), nullable=False)
        schema = pa.schema([pa.field("col", pa.large_list(outer_field), nullable=False)])
        result = ArrowDigester.hash_schema(schema).hex()
        assert result == "0000023014336b70e541ea20913795de44594938a32b325f17b5f03ed6ebf09859a95a"


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
        assert result == "000002a89c186dbf815b7192ed4ac574196d1e0203db10d53dd7d16fbe12fbbdbb651e"


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
        assert result == "000002a6a3ef288f2975f6a75b0336b2b7d69ffb4a2d129046710b7184d1eb76a45fe2"


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
        assert result == "000002a07f8d13692700d7b2cad515d343a2a1ce4b3dd7deac50316102a43638b496d2"


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
        batch = _record_batch([list_array], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002411fa81d41e904d832eceea8e2041fc96bda4e4dc58e72b76380e086d400a008"


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
        assert result == "000002aaafa89ee39b32668aeb6f65c56501061b7194257ea88f14c3be0eca314f1d0d"


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
        batch1 = _record_batch([s1], schema=schema)

        s2 = pa.StructArray.from_arrays(
            [pa.array([3], type=pa.int32()), pa.array([30], type=pa.int32())],
            names=["x", "y"],
        )
        batch2 = _record_batch([s2], schema=schema)

        d = ArrowDigester(schema)
        d.update(batch1)
        d.update(batch2)
        result = d.finalize().hex()
        assert result == "0000025d4b5b129c220a3171a616f1ff060a00fdb3647a34434f4d83e9d0a68502c334"


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
        batch1 = _record_batch([arr1], schema=schema)

        arr2 = pa.array(
            [[3, 4, 5]],
            type=list_type,
        )
        batch2 = _record_batch([arr2], schema=schema)

        d = ArrowDigester(schema)
        d.update(batch1)
        d.update(batch2)
        result = d.finalize().hex()
        assert result == "00000248df16aca0c1f681b8b02cd0c859b3073c238101a6893bfa44d9f536a9819870"


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
        assert result == "0000023e818d98ed740fcd8240f4fdb39e4c53581b071c9ab6200a27924c186414f178"

    def test_non_nullable_inner_field(self):
        """LargeList<Struct> with nullable=False item field.

        ids=[1,2,3,4], labels=['a','b','c','d'], offsets=[0,2,4]
        inner_field.nullable=False → no BitVec in structural-only entry.
        Golden value confirmed against Rust (PLT-1048).
        """
        result = ArrowDigester.hash_array(self._make_list_array(inner_nullable=False)).hex()
        assert result == "0000021a7aa4df873b7627617a5ce9a234a8849202508afc27eda81af271d8b12bf592"

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
        assert result == "000002b8e0f66c036e26098ba55673ee856b1b23fb6dcb9acfb63207cf8ad6fadc34dd"


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

        batch = _record_batch([name_arr, coords_arr, readings_arr], schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002e86fcf5b5a6b21b9f3f4a5f3eb6608c6e531ecabd95cbf365d01089d2a924619"

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
        assert result == "000002b4caaae1356e990fdb4b8ed43bf8f67bcdc3ee2f094da6e0f71575dc52ec31c9"
