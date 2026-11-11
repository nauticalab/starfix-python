"""Golden-hash parity tests: Python must produce byte-for-byte identical hashes to Rust.

Hash format 2: every expected value is a Rust-produced format-2 value from
tests/golden/format1_to_format2.json (ITL-898); the format-1 values these tests
pinned originally map to them one to one.
The Rust values are the authoritative source of truth; these tests fail
immediately if the Python implementation diverges.

Covers 36 distinct Arrow tables / arrays:
  - spec examples A, B, C, D, E, F, G, H, I, J, K, N, O
  - all primitive scalar types (bool, int8..uint64, float32, float64)
  - temporal types (date32, date64, time32(s/ms), time64(us/ns))
  - variable-length (binary, string/utf8)
  - decimal128 (precision ranges 1-9, 10-18, 19-38)
  - list-of-int32
  - struct column, nested struct (two levels), struct with nullable children
  - multi-type record batch, schema-only (empty) hash
  - streaming two-batch incremental update
"""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pytest
from starfix.arrow_digester import ArrowDigester


# ---------------------------------------------------------------------------
# Spec examples (byte-exact conformance, letter-for-letter from spec)
# ---------------------------------------------------------------------------


class TestSpecExamples:
    def test_example_a_two_column_table(self):
        """Example A: {age: Int32 non-null, name: LargeUtf8 nullable} — two rows."""
        schema = pa.schema([
            pa.field("age", pa.int32(), nullable=False),
            pa.field("name", pa.large_utf8(), nullable=True),
        ])
        batch = pa.record_batch(
            {
                "age": pa.array([25, 30], type=pa.int32()),
                "name": pa.array(["Alice", None], type=pa.large_utf8()),
            },
            schema=schema,
        )
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "00000292eeae0418072b559ac1b864e6a7e249749582ab32239ffc9443a00dcc18946f"

    def test_example_b_bool_array_with_nulls(self):
        """Example B: BooleanArray [true, NULL, false, true]."""
        array = pa.array([True, None, False, True], type=pa.bool_())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000024100d139820e2b287fee67fbd7624e103a6ab7674b5e0170e933a896cc9ee41d"

    def test_example_c_non_nullable_int32(self):
        """Example C: Int32Array [1, 2, 3] — non-nullable."""
        array = pa.array([1, 2, 3], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000024c470976f10386fad05c0ef8ac4d3f82c4fd656c3ea6255ef8a21180ce53597a"

    def test_example_d_non_nullable_binary(self):
        """Example D: BinaryArray [b'hi', b''] — non-nullable; tests Binary→LargeBinary canonicalization."""
        array = pa.array([b"hi", b""], type=pa.binary())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002c1577257ed34403943a24dcf25bb9f89a4034cc197e7d6ae03d68d46aacfebb1"

    def test_example_e_column_order_independence(self):
        """Example E: columns [x: Int32, y: Boolean nullable] — order must not matter."""
        schema_xy = pa.schema([
            pa.field("x", pa.int32(), nullable=False),
            pa.field("y", pa.bool_(), nullable=True),
        ])
        schema_yx = pa.schema([
            pa.field("y", pa.bool_(), nullable=True),
            pa.field("x", pa.int32(), nullable=False),
        ])
        batch_xy = pa.record_batch(
            {"x": pa.array([10], type=pa.int32()), "y": pa.array([True], type=pa.bool_())},
            schema=schema_xy,
        )
        batch_yx = pa.record_batch(
            {"y": pa.array([True], type=pa.bool_()), "x": pa.array([10], type=pa.int32())},
            schema=schema_yx,
        )
        expected = "00000259305d2093f85b576989659d7bbdb725b33653d8b4cffd65049ba88ac7d1b5ae"
        assert ArrowDigester.hash_record_batch(batch_xy).hex() == expected
        assert ArrowDigester.hash_record_batch(batch_yx).hex() == expected

    def test_example_f_utf8_equivalence(self):
        """Example F: Utf8 and LargeUtf8 arrays with the same data must hash identically."""
        small = pa.array(["ab"], type=pa.utf8())
        large = pa.array(["ab"], type=pa.large_utf8())
        expected = "00000217476a036ab2e8fecbae7ec4587d41fb06aef8bf552cf8f0b368dc41857d4286"
        assert ArrowDigester.hash_array(small).hex() == expected
        assert ArrowDigester.hash_array(large).hex() == expected

    def test_example_g_nullable_int32(self):
        """Example G: Int32Array [Some(42), None, Some(-7), Some(0)]."""
        array = pa.array([42, None, -7, 0], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002d1d8a0c525ec9d5b68cd7f3cc7c0ebf465c9159ae35a59010a18044e245e4f61"

    def test_example_h_nullable_string(self):
        """Example H: StringArray [Some('hello'), None, Some('world'), Some('')]."""
        array = pa.array(["hello", None, "world", ""], type=pa.utf8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002445ec6b02f286788f52986673493d9d2c7d2efe4b4870fa780aeec77fbaa0385"

    def test_example_i_empty_table(self):
        """Example I: schema {a: Int32 non-null, b: Boolean nullable} with zero rows."""
        schema = pa.schema([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.bool_(), nullable=True),
        ])
        digester = ArrowDigester(schema)
        result = digester.finalize().hex()
        assert result == "0000026bc41a0a4c6ef5d3b5168cac73a2961607896ad6f6801c632390662b48f03b84"

    def test_example_j_multi_batch_streaming(self):
        """Example J: batch-split independence — two batches == one combined batch."""
        schema = pa.schema([pa.field("v", pa.int32(), nullable=False)])
        batch1 = pa.record_batch({"v": pa.array([1, 2], type=pa.int32())}, schema=schema)
        batch2 = pa.record_batch({"v": pa.array([3], type=pa.int32())}, schema=schema)
        digester = ArrowDigester(schema)
        digester.update(batch1)
        digester.update(batch2)
        result = digester.finalize().hex()
        assert result == "000002a87ff648c2385327185b77630ffff50ae4470f90542582a783aa6740dcccb68b"

    def test_example_k_struct_column(self):
        """Example K: {person: Struct<age: Int32 non-null, name: LargeUtf8 non-null>}."""
        person_type = pa.struct([
            pa.field("age", pa.int32(), nullable=False),
            pa.field("name", pa.large_utf8(), nullable=False),
        ])
        schema = pa.schema([pa.field("person", person_type, nullable=False)])
        person_col = pa.array(
            [{"age": 25, "name": "Alice"}, {"age": 30, "name": "Bob"}],
            type=person_type,
        )
        batch = pa.record_batch({"person": person_col}, schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002be47c5bd05462dc3c4bbf0f62e5c14c2797f5ac40126e455342b5ff0c24e53d0"

    def test_example_n_list_of_struct(self):
        """Example N: {items: LargeList<Struct<id: Int32, label: LargeUtf8>> nullable}.

        Row 0: [{id:1, label:'a'}, {id:2, label:'b'}]
        Row 1: [{id:3, label:'c'}]
        """
        item_type = pa.struct([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("label", pa.large_utf8(), nullable=False),
        ])
        schema = pa.schema([
            pa.field("items", pa.large_list(pa.field("item", item_type, nullable=False)), nullable=True)
        ])
        items_col = pa.array(
            [
                [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}],
                [{"id": 3, "label": "c"}],
            ],
            type=pa.large_list(pa.field("item", item_type, nullable=False)),
        )
        batch = pa.record_batch({"items": items_col}, schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "00000287065f0798e1fac05078eded05303f6046343a29cd1315c3ae2dd60cffa40361"

    def test_example_o_nested_struct(self):
        """Example O: {s: Struct<a: Int32, nested: Struct<p: Int32, q: Int32>>} — two levels."""
        nested_type = pa.struct([
            pa.field("p", pa.int32(), nullable=False),
            pa.field("q", pa.int32(), nullable=False),
        ])
        outer_type = pa.struct([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("nested", nested_type, nullable=False),
        ])
        schema = pa.schema([pa.field("s", outer_type, nullable=False)])
        s_col = pa.array(
            [{"a": 10, "nested": {"p": 20, "q": 30}}],
            type=outer_type,
        )
        batch = pa.record_batch({"s": s_col}, schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "00000260e109b8c08b6265051d1f339801898ab5abc4dd6d874e628ef8bd6219492e0c"


# ---------------------------------------------------------------------------
# Primitive scalar types (all 11 numeric/bool types)
# ---------------------------------------------------------------------------


class TestPrimitiveTypes:
    def test_bool(self):
        array = pa.array([True, False, None, True], type=pa.bool_())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000259cf85678c3695d3ed17046b5a42d629606ab496949b33e9c96f675514a7937f"

    def test_int8(self):
        array = pa.array([1, None, -1, 127], type=pa.int8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000021fa6009005695174467a940479ad60dff4c4c25215b67eb6b9fbce06a39e9d7a"

    def test_int16(self):
        array = pa.array([1000, None, -1000, 32767], type=pa.int16())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002be4d10e5334ca6eddf6ef2a97068569ee8b4dbac275cfd025716bc845ad7fe73"

    def test_int32(self):
        array = pa.array([42, None, -7, 0], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002d1d8a0c525ec9d5b68cd7f3cc7c0ebf465c9159ae35a59010a18044e245e4f61"

    def test_int64(self):
        array = pa.array([1_000_000, None, -1_000_000, 0], type=pa.int64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000023aab6936af9bf10b02d01c46db095dd95e78d19b61cbdf5719f751f12a6a4d05"

    def test_uint8(self):
        array = pa.array([0, 1, None, 255], type=pa.uint8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002d4b5b7d8d8cdff6578c2ab3548ac28820d55e4bae62546c06cfb783eb96c7e0d"

    def test_uint16(self):
        array = pa.array([0, 1000, None, 65535], type=pa.uint16())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000025c7c3b1ecd630160eb0cee63321bd7135b4eef454e147691b3c5877c219781fc"

    def test_uint32(self):
        array = pa.array([0, 1_000_000, None, 4_294_967_295], type=pa.uint32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000025b3440e7326d5cf3dc08560b5db09d7c7099ff9578114eedf5e8721cc308c389"

    def test_uint64(self):
        array = pa.array([0, 1_000_000_000, None, 18_446_744_073_709_551_615], type=pa.uint64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000200b680c62c236f5ff1b973b82177922da1b1de2565c2ab051fe41ac7ada7520a"

    def test_float32(self):
        array = pa.array([1.5, None, -3.14, 0.0], type=pa.float32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000022b6984cbc7c3f8087433f36e850b77185e90f4b8874d5cdf5b4835fd05ee1fd2"

    def test_float64(self):
        array = pa.array([1.5, None, -3.14, 0.0], type=pa.float64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000028e4f1d81093e5dc9803b9ff7ba4272fbef115c7f5b79ce27ba1d607cec28abda"


# ---------------------------------------------------------------------------
# Temporal types
# ---------------------------------------------------------------------------


class TestTemporalTypes:
    def test_date32(self):
        """Date32: days since Unix epoch."""
        array = pa.array([0, 18993, None, -1], type=pa.date32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000027ee217bff78ba6d2162a3db9a312cda38eab812fa04b2bdae45c44caef53310a"

    def test_date64(self):
        """Date64: ms since Unix epoch."""
        array = pa.array([0, 1_640_995_200_000, None, -86_400_000], type=pa.date64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000243d93d2e644621c13ea350b6ce991e5183713cce06cccc78fa67bd45f28bfec8"

    def test_time32_seconds(self):
        """Time32(s): seconds since midnight."""
        array = pa.array([1000, None, 5000, 0], type=pa.time32("s"))
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000025ea1da45fe10e3034a18052542883c0c234353fc229ba1ab9423292d557fabe5"

    def test_time32_milliseconds(self):
        """Time32(ms): milliseconds since midnight."""
        array = pa.array([1_000_000, None, 5_000_000, 0], type=pa.time32("ms"))
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000246464b1335534e9caa69343551ad1f7eefe77df8a8201468eb1f5f8a469892dd"

    def test_time64_microseconds(self):
        """Time64(us): microseconds since midnight."""
        array = pa.array([1_000_000, None, 5_000_000, 0], type=pa.time64("us"))
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000025360527b27a5170c14a2f6e4493d940d29e27678d80899dd94655a4884a5589b"

    def test_time64_nanoseconds(self):
        """Time64(ns): nanoseconds since midnight."""
        array = pa.array([1_000_000_000, None, 5_000_000_000, 0], type=pa.time64("ns"))
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000025ba5fbd24942a70134a20ba575e71d66d54d3144872745d3239864baf8378aa1"


# ---------------------------------------------------------------------------
# Variable-length types
# ---------------------------------------------------------------------------


class TestVariableLengthTypes:
    def test_binary(self):
        """Binary array — tests LargeBinary canonicalization."""
        array = pa.array([b"hello", None, b"world", b""], type=pa.binary())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002ce0c8cc68b264f3040513d6fb5a94e4ec6b4b53a2baaad3cc0fd4d5a1b5e3f3d"

    def test_large_binary_equals_binary(self):
        """LargeBinary and Binary arrays with same data must produce the same hash."""
        small = pa.array([b"hello", None, b"world", b""], type=pa.binary())
        large = pa.array([b"hello", None, b"world", b""], type=pa.large_binary())
        expected = "000002ce0c8cc68b264f3040513d6fb5a94e4ec6b4b53a2baaad3cc0fd4d5a1b5e3f3d"
        assert ArrowDigester.hash_array(small).hex() == expected
        assert ArrowDigester.hash_array(large).hex() == expected

    def test_string(self):
        """String (Utf8) array — tests LargeUtf8 canonicalization."""
        array = pa.array(["hello", None, "world", ""], type=pa.utf8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002445ec6b02f286788f52986673493d9d2c7d2efe4b4870fa780aeec77fbaa0385"

    def test_large_string_equals_string(self):
        """LargeUtf8 and Utf8 arrays with same data must produce the same hash."""
        small = pa.array(["hello", None, "world", ""], type=pa.utf8())
        large = pa.array(["hello", None, "world", ""], type=pa.large_utf8())
        expected = "000002445ec6b02f286788f52986673493d9d2c7d2efe4b4870fa780aeec77fbaa0385"
        assert ArrowDigester.hash_array(small).hex() == expected
        assert ArrowDigester.hash_array(large).hex() == expected


# ---------------------------------------------------------------------------
# Decimal types (precision routing: 1-9, 10-18, 19-38)
# ---------------------------------------------------------------------------


class TestDecimalTypes:
    def test_decimal_precision_9_scale_2(self):
        """Decimal128 precision=9 (maps to decimal32 storage range) scale=2.

        Rust stores raw i128 values [123, -456, 0]. In PyArrow, pa.decimal128(9, 2)
        interprets values as scaled decimals, so we use Decimal strings to get
        the same raw integer storage: 1.23 * 10^2 = 123, etc.
        """
        array = pa.array(
            [Decimal("1.23"), None, Decimal("-4.56"), Decimal("0.00")],
            type=pa.decimal128(9, 2),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002e83147c84038cb72cb3cc8e24e651e0491028c449cfe7f2a9d51ded84391ca40"

    def test_decimal_precision_15_scale_3(self):
        """Decimal128 precision=15 (maps to decimal64 range) scale=3."""
        array = pa.array(
            [Decimal("1234567890.123"), None, Decimal("-9876543.210"), Decimal("0.000")],
            type=pa.decimal128(15, 3),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000226d3b5c66b1fa3b3de097542369f89b2717f3026c778ce17d57817fbf2fec8f0"

    def test_decimal_precision_38_scale_5(self):
        """Decimal128 precision=38 scale=5 — full 16-byte integer path."""
        array = pa.array(
            [
                Decimal("1234567890123456789012.34567"),
                None,
                Decimal("-9876543210987654321098.76543"),
                Decimal("0.00000"),
            ],
            type=pa.decimal128(38, 5),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000026193b4a954e812702fc77e319e834483780ac424d0b54fa9bf7163c90a9ab163"


# ---------------------------------------------------------------------------
# List types
# ---------------------------------------------------------------------------


class TestListTypes:
    def test_list_of_int32(self):
        """List<Int32> — tests LargeList canonicalization and per-element length prefix."""
        array = pa.array(
            [
                [1, 2, 3],
                None,
                [4, 5],
                [6],
            ],
            type=pa.list_(pa.field("item", pa.int32(), nullable=True)),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002e3c042074d4a197ca8b2f7e5aeae99b9cc9010a53b6c63120086d4e9688fd533"

    def test_large_list_equals_list(self):
        """LargeList<Int32> with the same data must produce the same hash as List<Int32>."""
        small = pa.array(
            [[1, 2, 3], None, [4, 5], [6]],
            type=pa.list_(pa.field("item", pa.int32(), nullable=True)),
        )
        large = pa.array(
            [[1, 2, 3], None, [4, 5], [6]],
            type=pa.large_list(pa.field("item", pa.int32(), nullable=True)),
        )
        expected = "000002e3c042074d4a197ca8b2f7e5aeae99b9cc9010a53b6c63120086d4e9688fd533"
        assert ArrowDigester.hash_array(small).hex() == expected
        assert ArrowDigester.hash_array(large).hex() == expected


# ---------------------------------------------------------------------------
# Struct types
# ---------------------------------------------------------------------------


class TestStructTypes:
    def test_struct_with_nullable_children(self):
        """Non-nullable struct column {a: Int32 non-null, b: Boolean nullable}."""
        struct_type = pa.struct([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.bool_(), nullable=True),
        ])
        schema = pa.schema([pa.field("s", struct_type, nullable=False)])
        s_col = pa.array(
            [{"a": 1, "b": True}, {"a": 2, "b": False}, {"a": 3, "b": None}],
            type=struct_type,
        )
        batch = pa.record_batch({"s": s_col}, schema=schema)
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002ae1910889fd81f51e1506c4d911e04df5ac428834f4acd3a8e91ae4a65809351"

    def test_struct_field_order_independence(self):
        """Struct fields in different order must hash identically (order-independence)."""
        type_ab = pa.struct([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.bool_(), nullable=True),
        ])
        type_ba = pa.struct([
            pa.field("b", pa.bool_(), nullable=True),
            pa.field("a", pa.int32(), nullable=False),
        ])
        schema_ab = pa.schema([pa.field("s", type_ab, nullable=False)])
        schema_ba = pa.schema([pa.field("s", type_ba, nullable=False)])

        data_ab = pa.array(
            [{"a": 1, "b": True}, {"a": 2, "b": False}, {"a": 3, "b": None}],
            type=type_ab,
        )
        data_ba = pa.array(
            [{"b": True, "a": 1}, {"b": False, "a": 2}, {"b": None, "a": 3}],
            type=type_ba,
        )
        batch_ab = pa.record_batch({"s": data_ab}, schema=schema_ab)
        batch_ba = pa.record_batch({"s": data_ba}, schema=schema_ba)
        assert (
            ArrowDigester.hash_record_batch(batch_ab)
            == ArrowDigester.hash_record_batch(batch_ba)
        )


# ---------------------------------------------------------------------------
# Multi-type record batch
# ---------------------------------------------------------------------------


class TestMultiTypeBatch:
    def test_multi_type_record_batch(self):
        """One-row record batch covering all scalar and collection types."""
        schema = pa.schema([
            pa.field("bool", pa.bool_(), nullable=True),
            pa.field("int8", pa.int8(), nullable=False),
            pa.field("uint8", pa.uint8(), nullable=False),
            pa.field("int16", pa.int16(), nullable=False),
            pa.field("uint16", pa.uint16(), nullable=False),
            pa.field("int32", pa.int32(), nullable=False),
            pa.field("uint32", pa.uint32(), nullable=False),
            pa.field("int64", pa.int64(), nullable=False),
            pa.field("uint64", pa.uint64(), nullable=False),
            pa.field("float32", pa.float32(), nullable=False),
            pa.field("float64", pa.float64(), nullable=False),
            pa.field("date32", pa.date32(), nullable=False),
            pa.field("date64", pa.date64(), nullable=False),
            pa.field("time32_second", pa.time32("s"), nullable=False),
            pa.field("time32_millis", pa.time32("ms"), nullable=False),
            pa.field("time64_micro", pa.time64("us"), nullable=False),
            pa.field("time64_nano", pa.time64("ns"), nullable=False),
            pa.field("binary", pa.binary(), nullable=True),
            pa.field("large_binary", pa.large_binary(), nullable=True),
            pa.field("utf8", pa.utf8(), nullable=True),
            pa.field("large_utf8", pa.large_utf8(), nullable=True),
            pa.field("list", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=True),
            pa.field("large_list", pa.large_list(pa.field("item", pa.int32(), nullable=True)), nullable=True),
            pa.field("decimal128", pa.decimal128(38, 5), nullable=True),
        ])
        batch = pa.record_batch(
            {
                "bool": pa.array([True], type=pa.bool_()),
                "int8": pa.array([1], type=pa.int8()),
                "uint8": pa.array([1], type=pa.uint8()),
                "int16": pa.array([100], type=pa.int16()),
                "uint16": pa.array([100], type=pa.uint16()),
                "int32": pa.array([1000], type=pa.int32()),
                "uint32": pa.array([1000], type=pa.uint32()),
                "int64": pa.array([100_000], type=pa.int64()),
                "uint64": pa.array([100_000], type=pa.uint64()),
                "float32": pa.array([1.5], type=pa.float32()),
                "float64": pa.array([1.5], type=pa.float64()),
                "date32": pa.array([18993], type=pa.date32()),
                "date64": pa.array([1_640_995_200_000], type=pa.date64()),
                "time32_second": pa.array([3600], type=pa.time32("s")),
                "time32_millis": pa.array([3_600_000], type=pa.time32("ms")),
                "time64_micro": pa.array([3_600_000_000], type=pa.time64("us")),
                "time64_nano": pa.array([3_600_000_000_000], type=pa.time64("ns")),
                "binary": pa.array([b"data1"], type=pa.binary()),
                "large_binary": pa.array([b"large1"], type=pa.large_binary()),
                "utf8": pa.array(["text1"], type=pa.utf8()),
                "large_utf8": pa.array(["large_text1"], type=pa.large_utf8()),
                "list": pa.array([[1, 2]], type=pa.list_(pa.field("item", pa.int32(), nullable=True))),
                "large_list": pa.array([[5, 6]], type=pa.large_list(pa.field("item", pa.int32(), nullable=True))),
                "decimal128": pa.array(
                    [Decimal("1234567890123456789012345.67890")],
                    type=pa.decimal128(38, 5),
                ),
            },
            schema=schema,
        )
        result = ArrowDigester.hash_record_batch(batch).hex()
        assert result == "000002915edae36a72a8337dd20f2536739344a71fb6d12cfbd7e15ba5813d9399f8ce"

    def test_multi_type_schema_only(self):
        """Empty (no-rows) digester over the large multi-type schema."""
        schema = pa.schema([
            pa.field("bool", pa.bool_(), nullable=True),
            pa.field("int8", pa.int8(), nullable=False),
            pa.field("uint8", pa.uint8(), nullable=False),
            pa.field("int16", pa.int16(), nullable=False),
            pa.field("uint16", pa.uint16(), nullable=False),
            pa.field("int32", pa.int32(), nullable=False),
            pa.field("uint32", pa.uint32(), nullable=False),
            pa.field("int64", pa.int64(), nullable=False),
            pa.field("uint64", pa.uint64(), nullable=False),
            pa.field("float32", pa.float32(), nullable=False),
            pa.field("float64", pa.float64(), nullable=False),
            pa.field("date32", pa.date32(), nullable=False),
            pa.field("date64", pa.date64(), nullable=False),
            pa.field("time32_second", pa.time32("s"), nullable=False),
            pa.field("time32_millis", pa.time32("ms"), nullable=False),
            pa.field("time64_micro", pa.time64("us"), nullable=False),
            pa.field("time64_nano", pa.time64("ns"), nullable=False),
            pa.field("binary", pa.binary(), nullable=True),
            pa.field("large_binary", pa.large_binary(), nullable=True),
            pa.field("utf8", pa.utf8(), nullable=True),
            pa.field("large_utf8", pa.large_utf8(), nullable=True),
            pa.field("list", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=True),
            pa.field("large_list", pa.large_list(pa.field("item", pa.int32(), nullable=True)), nullable=True),
            pa.field("decimal128", pa.decimal128(38, 5), nullable=True),
        ])
        result = ArrowDigester(schema).finalize().hex()
        assert result == "00000237415d17fd5904b31ed46c94418a47339c656e6270fb1a065479cf153d4641ee"


# ---------------------------------------------------------------------------
# Streaming / incremental hashing
# ---------------------------------------------------------------------------


class TestStreamingHashing:
    def test_streaming_two_batches(self):
        """Two-batch incremental update must match the known Rust hash."""
        schema = pa.schema([
            pa.field("uids", pa.int32(), nullable=False),
            pa.field("flags", pa.bool_(), nullable=True),
        ])
        batch1 = pa.record_batch(
            {
                "uids": pa.array([1, 2, 3, 4], type=pa.int32()),
                "flags": pa.array([True, False, None, True], type=pa.bool_()),
            },
            schema=schema,
        )
        batch2 = pa.record_batch(
            {
                "uids": pa.array([5, 6, 7, 8], type=pa.int32()),
                "flags": pa.array([False, True, True, None], type=pa.bool_()),
            },
            schema=schema,
        )
        digester = ArrowDigester(schema)
        digester.update(batch1)
        digester.update(batch2)
        result = digester.finalize().hex()
        assert result == "000002cfdbea6ff7282f0edaf4d7ca6015565dec2c8eb05073aeb6452ff83414cb8a43"

    def test_batch_split_independence(self):
        """Splitting a batch into two must not change the hash."""
        schema = pa.schema([
            pa.field("v", pa.int32(), nullable=False),
        ])
        combined = pa.record_batch({"v": pa.array([1, 2, 3], type=pa.int32())}, schema=schema)
        batch1 = pa.record_batch({"v": pa.array([1, 2], type=pa.int32())}, schema=schema)
        batch2 = pa.record_batch({"v": pa.array([3], type=pa.int32())}, schema=schema)

        hash_combined = ArrowDigester.hash_record_batch(combined)

        digester = ArrowDigester(schema)
        digester.update(batch1)
        digester.update(batch2)
        hash_split = digester.finalize()

        assert hash_combined == hash_split


# ---------------------------------------------------------------------------
# Timestamp and Duration hashing — golden values match Rust (ITL-438)
# ---------------------------------------------------------------------------


class TestTimestampDurationHashing:
    """Golden-hash parity for timestamp and duration types.

    All expected values were generated by the Rust starfix test suite
    (timestamp_array_hashing / duration_array_hashing in tests/arrow_digester.rs).
    Array contents: [0, None, 1_000] as raw i64 values.
    """

    # -- Timestamp (tz-aware UTC) ------------------------------------------

    def test_timestamp_second_utc(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("s", tz="UTC"))
        assert ArrowDigester.hash_array(arr).hex() == "00000252859a0dad006bac78dd1ba3fa17fde43bfa2af9cc95bdd08b6611da1691f012"

    def test_timestamp_millisecond_utc(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("ms", tz="UTC"))
        assert ArrowDigester.hash_array(arr).hex() == "000002f6dcd4d91471e975d86257785c15f0d8e120a84b45f163378e913d10935f1ec9"

    def test_timestamp_microsecond_utc(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("us", tz="UTC"))
        assert ArrowDigester.hash_array(arr).hex() == "00000209c1c989349caa973563a880521cbcaaa4e62ccc77f380e69f0fc57c639b4bfa"

    def test_timestamp_nanosecond_utc(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("ns", tz="UTC"))
        assert ArrowDigester.hash_array(arr).hex() == "0000021d86082dfda0e2f0449320c2acd853756d79dc3080cf1ee14327c7c9f2201480"

    # -- Timestamp (tz-naive) ----------------------------------------------

    def test_timestamp_second_naive(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("s"))
        assert ArrowDigester.hash_array(arr).hex() == "0000028fa5d6fbc775327e0c766cab77a00991dc87cf7ac7d45f875d049559b55cbb3e"

    def test_timestamp_millisecond_naive(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("ms"))
        assert ArrowDigester.hash_array(arr).hex() == "000002c0d0e86c4e7ff759ad83bf887cf5b20f2b662ceeeba5865ed1f5353b6b7a9445"

    def test_timestamp_microsecond_naive(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("us"))
        assert ArrowDigester.hash_array(arr).hex() == "000002148a265e62ae6878931d8d3316c4aec43f50da2e688d83bfe62901c3703e6b4f"

    def test_timestamp_nanosecond_naive(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("ns"))
        assert ArrowDigester.hash_array(arr).hex() == "00000292c1b50fa17e537e6afb5903cce37b5b28b03088e6ec3e23c346d9adafb071a7"

    # -- Timestamp: unit and tz produce different hashes -------------------

    def test_timestamp_units_differ(self):
        values = [1_000, 2_000]
        hashes = [
            ArrowDigester.hash_array(pa.array(values, type=pa.timestamp("s"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.timestamp("ms"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.timestamp("us"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.timestamp("ns"))).hex(),
        ]
        assert len(set(hashes)) == 4, "all 4 timestamp units must produce distinct hashes"

    def test_timestamp_tz_differs(self):
        values = [1_000, 2_000]
        naive = ArrowDigester.hash_array(pa.array(values, type=pa.timestamp("us"))).hex()
        utc = ArrowDigester.hash_array(
            pa.array(values, type=pa.timestamp("us", tz="UTC"))
        ).hex()
        assert naive != utc, "tz-naive and tz=UTC must produce different hashes"

    # -- Duration ----------------------------------------------------------

    def test_duration_second(self):
        arr = pa.array([0, None, 1_000], type=pa.duration("s"))
        assert ArrowDigester.hash_array(arr).hex() == "000002f4db00404adc309eaa3c23518b36b56a44659edc0a4c4dbbddea90449cf90413"

    def test_duration_millisecond(self):
        arr = pa.array([0, None, 1_000], type=pa.duration("ms"))
        assert ArrowDigester.hash_array(arr).hex() == "000002767dcb630f550d41da343f63fb308d1e2ce97e68306f988573178da666fb156f"

    def test_duration_microsecond(self):
        arr = pa.array([0, None, 1_000], type=pa.duration("us"))
        assert ArrowDigester.hash_array(arr).hex() == "00000267a46b80b8577b61b24e665edbe71f78d6af7170305cbe0b348ca07a1b2874d9"

    def test_duration_nanosecond(self):
        arr = pa.array([0, None, 1_000], type=pa.duration("ns"))
        assert ArrowDigester.hash_array(arr).hex() == "00000265d49688aae1ade659148d969f4fd703fa688fd7e3dcbe3c00d5e7ef2ae50720"

    def test_duration_units_differ(self):
        values = [1_000, 2_000]
        hashes = [
            ArrowDigester.hash_array(pa.array(values, type=pa.duration("s"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.duration("ms"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.duration("us"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.duration("ns"))).hex(),
        ]
        assert len(set(hashes)) == 4, "all 4 duration units must produce distinct hashes"
