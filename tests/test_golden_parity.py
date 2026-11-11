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
        assert result == "000002d3ab7e2b2ff00fc12767f712e8e3f1e941d4616af2d44b3a5ded0675afd31207"

    def test_example_b_bool_array_with_nulls(self):
        """Example B: BooleanArray [true, NULL, false, true]."""
        array = pa.array([True, None, False, True], type=pa.bool_())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000024c5223c9cdc2fe10fb4362890cfc763e1bea4829a30fa6bfedc7bacad05f783a"

    def test_example_c_non_nullable_int32(self):
        """Example C: Int32Array [1, 2, 3] — non-nullable."""
        array = pa.array([1, 2, 3], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000028e4c60ec932ba53b9f514d4f6953091bed36d7295ad1d4cdde10907b83bf550f"

    def test_example_d_non_nullable_binary(self):
        """Example D: BinaryArray [b'hi', b''] — non-nullable; tests Binary→LargeBinary canonicalization."""
        array = pa.array([b"hi", b""], type=pa.binary())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000020ee499da085a532f069ccfe218a9af0f2fa33ab8cc21b79991bdc36f5323f141"

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
        expected = "00000201b7c533b770a31fd7bc0fefa6252c2fa7100305f07f991502437cc1a5ba1d24"
        assert ArrowDigester.hash_record_batch(batch_xy).hex() == expected
        assert ArrowDigester.hash_record_batch(batch_yx).hex() == expected

    def test_example_f_utf8_equivalence(self):
        """Example F: Utf8 and LargeUtf8 arrays with the same data must hash identically."""
        small = pa.array(["ab"], type=pa.utf8())
        large = pa.array(["ab"], type=pa.large_utf8())
        expected = "000002644b2970be7606407afad7635d7f7111c2f668496e938bdff452604bd444065b"
        assert ArrowDigester.hash_array(small).hex() == expected
        assert ArrowDigester.hash_array(large).hex() == expected

    def test_example_g_nullable_int32(self):
        """Example G: Int32Array [Some(42), None, Some(-7), Some(0)]."""
        array = pa.array([42, None, -7, 0], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000264c8fe45cb550e25577276b2af8c1c9c125bc14b8439f5619c1f18c71d4d0078"

    def test_example_h_nullable_string(self):
        """Example H: StringArray [Some('hello'), None, Some('world'), Some('')]."""
        array = pa.array(["hello", None, "world", ""], type=pa.utf8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000020580567a1ed9be7b4dbbc36046e635c7021fb1bda241310cd353a3de5a860bc2"

    def test_example_i_empty_table(self):
        """Example I: schema {a: Int32 non-null, b: Boolean nullable} with zero rows."""
        schema = pa.schema([
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.bool_(), nullable=True),
        ])
        digester = ArrowDigester(schema)
        result = digester.finalize().hex()
        assert result == "0000028aa5ce23c0faa5de71df6c33cb88194f8555ce4c2ddf8398ebd15e7933081798"

    def test_example_j_multi_batch_streaming(self):
        """Example J: batch-split independence — two batches == one combined batch."""
        schema = pa.schema([pa.field("v", pa.int32(), nullable=False)])
        batch1 = pa.record_batch({"v": pa.array([1, 2], type=pa.int32())}, schema=schema)
        batch2 = pa.record_batch({"v": pa.array([3], type=pa.int32())}, schema=schema)
        digester = ArrowDigester(schema)
        digester.update(batch1)
        digester.update(batch2)
        result = digester.finalize().hex()
        assert result == "0000027d2ac950396ddef47becbd1f9625b58f0419b7ec5fe779bd62a6ee046b5d9a27"

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
        assert result == "000002b9461e7b30083397b2a328415184d53e8c27d315af89e22163aae5a0da91866a"

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
        assert result == "000002a58f04b6bc45c95e5faa11155031bfb9dc8815187dde82b0f4d833cb8f376056"

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
        assert result == "000002ecebdb320ae446cd5347ec7cbff5272c36af957666f717c59df85cfd8410d5a4"


# ---------------------------------------------------------------------------
# Primitive scalar types (all 11 numeric/bool types)
# ---------------------------------------------------------------------------


class TestPrimitiveTypes:
    def test_bool(self):
        array = pa.array([True, False, None, True], type=pa.bool_())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002b4fe586ae0a97fcc8ae84e83110f8980bde7c74aed4c66b60321116994982aeb"

    def test_int8(self):
        array = pa.array([1, None, -1, 127], type=pa.int8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000024168dcfaf09c69c2fd848090fcff65c6e27c7a8575f419e6b5c011b28633c6ec"

    def test_int16(self):
        array = pa.array([1000, None, -1000, 32767], type=pa.int16())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000021193e13fb74196a3ba1e2848d8b371cbaec0145408be4c10dce5c7da3d6f0ef8"

    def test_int32(self):
        array = pa.array([42, None, -7, 0], type=pa.int32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000264c8fe45cb550e25577276b2af8c1c9c125bc14b8439f5619c1f18c71d4d0078"

    def test_int64(self):
        array = pa.array([1_000_000, None, -1_000_000, 0], type=pa.int64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000273f6a64dd00ba1e2749c0ff21c64bb1984989efb7f94e37b188e6f0ea0ee3607"

    def test_uint8(self):
        array = pa.array([0, 1, None, 255], type=pa.uint8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002f5401303c3c8609dde1c1162a0a5388183231ae9d7aad98a5165ca6cb949e474"

    def test_uint16(self):
        array = pa.array([0, 1000, None, 65535], type=pa.uint16())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000023c4a18350b71b2498685b732bf86aafa7edc9b6aea24de50e6a764cb4f0a8ec6"

    def test_uint32(self):
        array = pa.array([0, 1_000_000, None, 4_294_967_295], type=pa.uint32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002aa9b7f8fdea28a5651544e73648faa19f22b87912a4b18ec121df884a65e3440"

    def test_uint64(self):
        array = pa.array([0, 1_000_000_000, None, 18_446_744_073_709_551_615], type=pa.uint64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002bab5700cfdadfbc1a63b32bbab4c8154ad9696f024e6965266b10714d18ba772"

    def test_float32(self):
        array = pa.array([1.5, None, -3.14, 0.0], type=pa.float32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000279154d63290b405df71342d15c41dd9f288d5bd1dadae43f37950b85dcf74041"

    def test_float64(self):
        array = pa.array([1.5, None, -3.14, 0.0], type=pa.float64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002f2dd5acce604d642d652fbbbc3bfb16807f36100aff5a84d1f6acb194e4d90e9"


# ---------------------------------------------------------------------------
# Temporal types
# ---------------------------------------------------------------------------


class TestTemporalTypes:
    def test_date32(self):
        """Date32: days since Unix epoch."""
        array = pa.array([0, 18993, None, -1], type=pa.date32())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000248ddc5205e534b4a5dd91fb3ce5b5c4cddff659844e77de731435ab8eda086cc"

    def test_date64(self):
        """Date64: ms since Unix epoch."""
        array = pa.array([0, 1_640_995_200_000, None, -86_400_000], type=pa.date64())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "00000273f2b0aeedb498d6980083f025b8e0c07c74c57c9130aabfed2af1b31e5ee0af"

    def test_time32_seconds(self):
        """Time32(s): seconds since midnight."""
        array = pa.array([1000, None, 5000, 0], type=pa.time32("s"))
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000026708f2d0b632bf3f324716cd32c7c41dd8b9bc75d326fb5ec3f3900201f08eb7"

    def test_time32_milliseconds(self):
        """Time32(ms): milliseconds since midnight."""
        array = pa.array([1_000_000, None, 5_000_000, 0], type=pa.time32("ms"))
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000028211097939067b80d7b9d622b88201aedc86254255f02f513e3acbf8da34e04a"

    def test_time64_microseconds(self):
        """Time64(us): microseconds since midnight."""
        array = pa.array([1_000_000, None, 5_000_000, 0], type=pa.time64("us"))
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000022ddc0ab9c1cfc0fda9353d66612a504cb63d6ee509c4f56b961f33855f1a82a4"

    def test_time64_nanoseconds(self):
        """Time64(ns): nanoseconds since midnight."""
        array = pa.array([1_000_000_000, None, 5_000_000_000, 0], type=pa.time64("ns"))
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000020ec4f4365fc43e57f5a229faf87d5e90dbe3ff53f64844c7bae443bb5c35bf07"


# ---------------------------------------------------------------------------
# Variable-length types
# ---------------------------------------------------------------------------


class TestVariableLengthTypes:
    def test_binary(self):
        """Binary array — tests LargeBinary canonicalization."""
        array = pa.array([b"hello", None, b"world", b""], type=pa.binary())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002e52c6b710b5c6be2cada5b225f0d43d83f2b0a89f117f9210eff746ebcd3f294"

    def test_large_binary_equals_binary(self):
        """LargeBinary and Binary arrays with same data must produce the same hash."""
        small = pa.array([b"hello", None, b"world", b""], type=pa.binary())
        large = pa.array([b"hello", None, b"world", b""], type=pa.large_binary())
        expected = "000002e52c6b710b5c6be2cada5b225f0d43d83f2b0a89f117f9210eff746ebcd3f294"
        assert ArrowDigester.hash_array(small).hex() == expected
        assert ArrowDigester.hash_array(large).hex() == expected

    def test_string(self):
        """String (Utf8) array — tests LargeUtf8 canonicalization."""
        array = pa.array(["hello", None, "world", ""], type=pa.utf8())
        result = ArrowDigester.hash_array(array).hex()
        assert result == "0000020580567a1ed9be7b4dbbc36046e635c7021fb1bda241310cd353a3de5a860bc2"

    def test_large_string_equals_string(self):
        """LargeUtf8 and Utf8 arrays with same data must produce the same hash."""
        small = pa.array(["hello", None, "world", ""], type=pa.utf8())
        large = pa.array(["hello", None, "world", ""], type=pa.large_utf8())
        expected = "0000020580567a1ed9be7b4dbbc36046e635c7021fb1bda241310cd353a3de5a860bc2"
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
        assert result == "0000021480fe62da952e0f819540738817488f0da864a59e70604f88f00f6daec3a7fb"

    def test_decimal_precision_15_scale_3(self):
        """Decimal128 precision=15 (maps to decimal64 range) scale=3."""
        array = pa.array(
            [Decimal("1234567890.123"), None, Decimal("-9876543.210"), Decimal("0.000")],
            type=pa.decimal128(15, 3),
        )
        result = ArrowDigester.hash_array(array).hex()
        assert result == "000002efb97088aa7fc1adc578e6d68d0f2d2dbe5ef25b0999fcd7e7208ede6e7b2a41"

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
        assert result == "0000029c8ebfcade372b53f23d669d231eab0dd2dd96805263867e47af72c739e1b455"


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
        assert result == "000002baac87674321616f3c4b4ad559df2c5b2492a83ba54fa7e865a51ae56d8f498c"

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
        expected = "000002baac87674321616f3c4b4ad559df2c5b2492a83ba54fa7e865a51ae56d8f498c"
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
        assert result == "00000223f577c71ad70b36baf8db3ae57304492642edeab6f98560180b624eb7127d40"

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
        assert result == "000002cb129d9295971fe47548a28b3070f18a4a000d449aa6e612446c37e7a104bdb2"

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
        assert result == "00000221f1240fabe97f742ddb3195193cf785f61e327b7d64ec7363bbbce399df7e50"


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
        assert result == "0000020ba4ed542e67ebc311d160ab54a11f25630a6b5fdd99e5dfc90917cb1d31e968"

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
        assert ArrowDigester.hash_array(arr).hex() == "00000255a8e82e6e1862a461c70c393f105522ee7fd6f4ed889fd69d21ace4bb54622e"

    def test_timestamp_millisecond_utc(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("ms", tz="UTC"))
        assert ArrowDigester.hash_array(arr).hex() == "0000026b928249c5b3e8a99fff770aa7cff273da347a8dd5ee5c1647e7a03b41aeeac5"

    def test_timestamp_microsecond_utc(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("us", tz="UTC"))
        assert ArrowDigester.hash_array(arr).hex() == "0000023277d54aec4f229bfd0f21e772c332d2d4851c8af07b3f36be29f655b91b6b48"

    def test_timestamp_nanosecond_utc(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("ns", tz="UTC"))
        assert ArrowDigester.hash_array(arr).hex() == "0000022d542c28b882c3e78bd1758a04c628c829efb9c16e78ad4185cd97e45544e988"

    # -- Timestamp (tz-naive) ----------------------------------------------

    def test_timestamp_second_naive(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("s"))
        assert ArrowDigester.hash_array(arr).hex() == "00000219b77ac863025bc3a0c519594e873846eaed63ed9d7c16763ad9eb36016623ed"

    def test_timestamp_millisecond_naive(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("ms"))
        assert ArrowDigester.hash_array(arr).hex() == "000002973baeb308b246159206a6072825f4cb786d05ddff11649199c93df22abdaac7"

    def test_timestamp_microsecond_naive(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("us"))
        assert ArrowDigester.hash_array(arr).hex() == "000002fe90da5f713e78f96eed1bf834e6fe2f871aa2c7f28174d8a2dcb3134f5dc4cc"

    def test_timestamp_nanosecond_naive(self):
        arr = pa.array([0, None, 1_000], type=pa.timestamp("ns"))
        assert ArrowDigester.hash_array(arr).hex() == "00000275a347fae688071e8d3d94d6ff48800f3dbbd9d8fc629c871eb4e16dd3e59976"

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
        assert ArrowDigester.hash_array(arr).hex() == "000002ac73fa834511e4de143e7f5da1f2b6bef168f25a6d47f7851dd977328eaf20cc"

    def test_duration_millisecond(self):
        arr = pa.array([0, None, 1_000], type=pa.duration("ms"))
        assert ArrowDigester.hash_array(arr).hex() == "000002cfc3e82b4f82cf9a39f2f86fb495acb6f481b91643bd8f5591a2ed34f794ee9a"

    def test_duration_microsecond(self):
        arr = pa.array([0, None, 1_000], type=pa.duration("us"))
        assert ArrowDigester.hash_array(arr).hex() == "0000022c8e105be42590d0cd332abcf8330269689cf66767322d1ba799271c6346744d"

    def test_duration_nanosecond(self):
        arr = pa.array([0, None, 1_000], type=pa.duration("ns"))
        assert ArrowDigester.hash_array(arr).hex() == "00000257be9b6546267351273ab5c9bd7858c17b17b093f7f65a599bad8bde37ab5c9e"

    def test_duration_units_differ(self):
        values = [1_000, 2_000]
        hashes = [
            ArrowDigester.hash_array(pa.array(values, type=pa.duration("s"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.duration("ms"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.duration("us"))).hex(),
            ArrowDigester.hash_array(pa.array(values, type=pa.duration("ns"))).hex(),
        ]
        assert len(set(hashes)) == 4, "all 4 duration units must produce distinct hashes"
