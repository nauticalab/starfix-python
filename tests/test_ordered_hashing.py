"""Tests for the v0.5 hashing options (ITL-898).

Spec: ``nauticalab/starfix docs/byte-layout-spec.md`` Section 9.

* ``ordered_columns``        (flag 0x01): top-level column order is part of the hash.
* ``ordered_struct_fields``  (flag 0x02): struct field order is part of the hash, at every depth.
* ``nested_validity``        (flag 0x04): nullable nested values record their own validity.

The worked examples (spec Section 9.9) are checked byte for byte: the expected value is built
here from the exact SHA-256 inputs the spec lists, never from the implementation under test.
Cross-language values are pinned separately by ``tests/test_golden_parity_ordered.py``.
"""

from __future__ import annotations

import hashlib
import itertools
import struct

import pyarrow as pa
import pytest

from starfix.arrow_digester import _OPTION_FLAGS, ArrowDigester

# Option keyword names. A rename of an option is a one-line change here.
OC = "ordered_columns"
OSF = "ordered_struct_fields"
NV = "nested_validity"

ALL_FLAGS = range(8)


def opts(flags: int) -> dict[str, bool]:
    """Keyword arguments for a flags byte (spec Section 9.1)."""
    return {name: bool(flags & bit) for name, bit in _OPTION_FLAGS.items()}


def tag(flags: int) -> bytes:
    """Options tag of spec Section 9.2, written out literally."""
    return bytes.fromhex("0073746172666978 2d6f7074696f6e73 2d7631".replace(" ", "")) + bytes([flags])


def sha(*parts: bytes) -> bytes:
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


def u64(n: int) -> bytes:
    return struct.pack("<Q", n)


def i32(*vals: int) -> bytes:
    return b"".join(struct.pack("<i", v) for v in vals)


V = b"\x00\x00\x01"


def batch_hash(batch: pa.RecordBatch, flags: int, **kw) -> bytes:
    return ArrowDigester.hash_record_batch(batch, **opts(flags), **kw)


# ── Spec Section 9.1 / 9.2: option table, tag ─────────────────────────


class TestOptionTable:
    def test_option_names_and_bits(self):
        assert _OPTION_FLAGS == {OC: 0x01, OSF: 0x02, NV: 0x04}

    def test_tag_is_twenty_bytes(self):
        t = tag(0x05)
        assert len(t) == 20
        assert t[0] == 0x00
        assert t[1:19] == b"starfix-options-v1"
        assert t[19] == 0x05


# ── Spec Section 9.9: worked examples, byte level ─────────────────────


class TestWorkedExamples:
    def test_t1_hash_schema_ordered_columns(self):
        schema = pa.schema([
            pa.field("b", pa.int32(), nullable=False),
            pa.field("a", pa.utf8(), nullable=True),
        ])
        json_ = (
            b'[{"data_type":"Int32","name":"b","nullable":false},'
            b'{"data_type":"LargeUtf8","name":"a","nullable":true}]'
        )
        expected = V + sha(tag(0x01), json_)
        assert ArrowDigester.hash_schema(schema, ordered_columns=True) == expected

    def test_t2_record_batch_ordered_columns(self):
        schema = pa.schema([
            pa.field("b", pa.int32(), nullable=False),
            pa.field("a", pa.int32(), nullable=False),
        ])
        batch = pa.record_batch(
            [pa.array([5], pa.int32()), pa.array([6], pa.int32())], schema=schema
        )
        schema_digest = sha(
            tag(0x01),
            b'[{"data_type":"Int32","name":"b","nullable":false},'
            b'{"data_type":"Int32","name":"a","nullable":false}]',
        )
        expected = V + sha(schema_digest, sha(i32(6)), sha(i32(5)))
        assert ArrowDigester.hash_record_batch(batch, ordered_columns=True) == expected

    def test_t3_hash_array_ordered_struct_fields(self):
        arr = pa.StructArray.from_arrays(
            [pa.array([1], pa.int32()), pa.array([2], pa.int32())],
            fields=[pa.field("z", pa.int32(), nullable=False), pa.field("a", pa.int32(), nullable=False)],
        )
        expected = V + sha(
            tag(0x02),
            b'{"Struct":[{"data_type":"Int32","name":"z","nullable":false},'
            b'{"data_type":"Int32","name":"a","nullable":false}]}',
            sha(i32(2)),
            sha(i32(1)),
        )
        assert ArrowDigester.hash_array(arr, ordered_struct_fields=True) == expected

    def test_t4_nested_validity_nullable_struct(self):
        s = pa.StructArray.from_arrays(
            [pa.array([5, None, 1], pa.int32())],
            fields=[pa.field("a", pa.int32(), nullable=True)],
            mask=pa.array([True, False, False]),  # True = null: struct validity [0, 1, 1]
        )
        schema = pa.schema([pa.field("s", s.type, nullable=True)])
        batch = pa.record_batch([s], schema=schema)
        schema_digest = sha(
            tag(0x04),
            b'{"s":{"data_type":{"Struct":[{"data_type":"Int32","name":"a","nullable":true}]},'
            b'"nullable":true}}',
        )
        expected = V + sha(
            schema_digest,
            u64(3) + b"\x06",  # entry "s": validity-only, bits [0,1,1]
            u64(3) + b"\x04",  # entry "s/a": [1,0,1] AND [0,1,1] = [0,0,1]
            sha(i32(1)),  #       data: only row 2
        )
        assert ArrowDigester.hash_record_batch(batch, nested_validity=True) == expected

    def test_t5_nested_validity_list_of_nullable_list(self):
        inner = pa.field("item", pa.int32(), nullable=True)
        outer_elem = pa.field("item", pa.list_(inner), nullable=True)
        col_type = pa.list_(outer_elem)
        schema = pa.schema([pa.field("c", col_type, nullable=False)])
        batch = pa.record_batch([pa.array([[[1], [2]]], type=col_type)], schema=schema)
        schema_digest = sha(
            tag(0x04),
            b'{"c":{"data_type":{"LargeList":{"data_type":{"LargeList":'
            b'{"data_type":"Int32","nullable":true}},"nullable":true}},"nullable":false}}',
        )
        expected = V + sha(
            schema_digest,
            # entry "c/": structural entry kept, null_bits = inner-list validity [1,1]
            u64(2) + b"\x03",
            sha(u64(2)),  # outer lengths [2]
            # entry "c//": list-leaf, item validity [1,1], inner lengths [1,1], data [1,2]
            u64(2) + b"\x03",
            sha(u64(1) + u64(1)),
            sha(i32(1, 2)),
        )
        assert ArrowDigester.hash_record_batch(batch, nested_validity=True) == expected

    def test_t6_nested_validity_boolean_column(self):
        schema = pa.schema([pa.field("b", pa.bool_(), nullable=True)])
        batch = pa.record_batch([pa.array([True, None, False])], schema=schema)
        schema_digest = sha(tag(0x04), b'{"b":{"data_type":"Boolean","nullable":true}}')
        expected = V + sha(
            schema_digest,
            u64(3) + b"\x05",  # validity [1,0,1]
            sha(b"\x01\x00"),  # one byte per VALID value: true, false
        )
        assert ArrowDigester.hash_record_batch(batch, nested_validity=True) == expected

    def test_t6_nested_validity_boolean_hash_array(self):
        arr = pa.array([True, False, True])
        expected = V + sha(tag(0x04), b'"Boolean"', sha(b"\x01\x00\x01"))
        assert ArrowDigester.hash_array(arr, nested_validity=True) == expected


# ── Spec Section 9.3: Phase 1 JSON for every flags value ──────────────


_STRUCT_SORTED = (
    b'{"Struct":[{"data_type":"Int32","name":"x","nullable":false},'
    b'{"data_type":"Boolean","name":"y","nullable":false}]}'
)
_STRUCT_DECLARED = (
    b'{"Struct":[{"data_type":"Boolean","name":"y","nullable":false},'
    b'{"data_type":"Int32","name":"x","nullable":false}]}'
)


class TestPhase1Json:
    """Spec 9.3 examples, checked as the exact bytes behind ``hash_schema``."""

    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if f])
    def test_struct_example_every_flags_value(self, flags):
        schema = pa.schema([
            pa.field("s", pa.struct([
                pa.field("y", pa.bool_(), nullable=False),
                pa.field("x", pa.int32(), nullable=False),
            ]), nullable=False),
        ])
        dt = _STRUCT_DECLARED if flags & 0x02 else _STRUCT_SORTED
        if flags & 0x01:
            json_ = b'[{"data_type":' + dt + b',"name":"s","nullable":false}]'
        else:
            json_ = b'{"s":{"data_type":' + dt + b',"nullable":false}}'
        assert ArrowDigester.hash_schema(schema, **opts(flags)) == V + sha(tag(flags), json_)

    def test_unordered_columns_with_flags_uses_object(self):
        schema = pa.schema([
            pa.field("b", pa.int32(), nullable=False),
            pa.field("a", pa.utf8(), nullable=True),
        ])
        json_ = (
            b'{"a":{"data_type":"LargeUtf8","nullable":true},'
            b'"b":{"data_type":"Int32","nullable":false}}'
        )
        assert ArrowDigester.hash_schema(schema, nested_validity=True) == V + sha(tag(0x04), json_)

    def test_ordered_struct_fields_inside_list_fixed_size_list_and_dictionary(self):
        st = pa.struct([pa.field("z", pa.int8(), nullable=False), pa.field("a", pa.int8(), nullable=False)])
        schema = pa.schema([
            pa.field("d", pa.dictionary(pa.int32(), pa.utf8()), nullable=True),
            pa.field("f", pa.list_(pa.field("item", st, nullable=True), 2), nullable=True),
            pa.field("l", pa.list_(pa.field("item", st, nullable=False)), nullable=False),
        ])
        za = (
            b'{"Struct":[{"data_type":"Int8","name":"z","nullable":false},'
            b'{"data_type":"Int8","name":"a","nullable":false}]}'
        )
        json_ = (
            b'{"d":{"data_type":"LargeUtf8","nullable":true},'
            b'"f":{"data_type":{"FixedSizeList":[{"data_type":' + za + b',"nullable":true},2]},"nullable":true},'
            b'"l":{"data_type":{"LargeList":{"data_type":' + za + b',"nullable":false}},"nullable":false}}'
        )
        assert ArrowDigester.hash_schema(schema, ordered_struct_fields=True) == V + sha(tag(0x02), json_)

    def test_non_ascii_names_are_raw_utf8(self):
        """serde_json writes non-ASCII as raw UTF-8 (no \\u escapes)."""
        st = pa.struct([pa.field("é", pa.int8(), nullable=False)])
        schema = pa.schema([pa.field("ü", st, nullable=False)])
        json_ = '[{"data_type":{"Struct":[{"data_type":"Int8","name":"é","nullable":false}]},"name":"ü","nullable":false}]'
        assert ArrowDigester.hash_schema(schema, ordered_columns=True) == V + sha(tag(0x01), json_.encode())

    def test_hash_array_type_json_non_ascii_raw_utf8(self):
        arr = pa.StructArray.from_arrays(
            [pa.array([1], pa.int8())], fields=[pa.field("é", pa.int8(), nullable=False)]
        )
        json_ = '{"Struct":[{"data_type":"Int8","name":"é","nullable":false}]}'.encode()
        expected = V + sha(tag(0x02), json_, sha(b"\x01"))
        assert ArrowDigester.hash_array(arr, ordered_struct_fields=True) == expected

    def test_metadata_phase2_appended_after_tagged_phase1(self):
        schema = pa.schema(
            [pa.field("b", pa.int32(), nullable=False, metadata={"unit": "kg"})],
            metadata={"version": "2"},
        )
        phase1 = b'[{"data_type":"Int32","name":"b","nullable":false}]'
        phase2 = b'{"fields":{"b":{"unit":"kg"}},"schema":{"version":"2"}}'
        got = ArrowDigester.hash_schema(schema, ordered_columns=True, include_metadata=True)
        assert got == V + sha(tag(0x01), phase1, phase2)


class TestSpecSection2Examples:
    """Literal examples added to spec Section 2.1 for ITL-898."""

    def test_fixed_size_list_example(self):
        schema = pa.schema([pa.field("f", pa.list_(pa.field("item", pa.int32(), nullable=True), 2), nullable=False)])
        json_ = b'{"f":{"data_type":{"FixedSizeList":[{"data_type":"Int32","nullable":true},2]},"nullable":false}}'
        expected = "000001faa6e75b09b2788440c79a1e297c73c75a5cb4d36c9f9cad6e3062a33c91c696"
        assert (V + sha(json_)).hex() == expected
        assert ArrowDigester.hash_schema(schema).hex() == expected

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_json_text_rules_control_characters(self, flags):
        """`a<LF>b<U+001B>` serializes as "a\\nb\\u001b"; '/' and U+007F stay raw."""
        schema = pa.schema([pa.field("a\nb\x1b/\x7f\"\\", pa.int8(), nullable=False)])
        name = b'"a\\nb\\u001b/\x7f\\"\\\\"'
        if flags & 0x01:
            json_ = b'[{"data_type":"Int8","name":' + name + b',"nullable":false}]'
        else:
            json_ = b"{" + name + b':{"data_type":"Int8","nullable":false}}'
        prefix = tag(flags) if flags else b""
        assert ArrowDigester.hash_schema(schema, **opts(flags)) == V + sha(prefix, json_)

class TestTypeFormsAndMetadataPathsWithOptions:
    """Spec 2.1 forms of Null/Interval/view types and 2.2.1 map metadata paths (options on)."""

    @pytest.mark.parametrize(
        "factory, form",
        [
            ("null", b'"Null"'),
            ("month_day_nano_interval", b'{"Interval":"MonthDayNano"}'),
            ("string_view", b'"Utf8View"'),
            ("binary_view", b'"BinaryView"'),
        ],
    )
    def test_type_forms(self, factory, form):
        if not hasattr(pa, factory):  # view types need pyarrow >= 16
            pytest.skip(f"pyarrow {pa.__version__} has no pa.{factory}()")
        dt = getattr(pa, factory)()
        schema = pa.schema([pa.field("c", dt, nullable=True)])
        json_ = b'[{"data_type":' + form + b',"name":"c","nullable":true}]'
        assert ArrowDigester.hash_schema(schema, ordered_columns=True) == V + sha(tag(0x01), json_)

    def test_default_mode_still_rejects_these_types(self):
        with pytest.raises(NotImplementedError):
            ArrowDigester.hash_schema(pa.schema([pa.field("c", pa.null())]))

    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if f])
    def test_metadata_paths_look_through_dictionaries(self, flags):
        """Spec 2.2.1 paths are the 9.6 node paths, which look through dictionaries."""
        st = pa.struct([pa.field("a", pa.int32(), metadata={"u": "kg"})])
        schema = pa.schema([pa.field("d", pa.dictionary(pa.int32(), st), nullable=True)])
        dt = b'{"Struct":[{"data_type":"Int32","name":"a","nullable":true}]}'
        if flags & 0x01:
            phase1 = b'[{"data_type":' + dt + b',"name":"d","nullable":true}]'
        else:
            phase1 = b'{"d":{"data_type":' + dt + b',"nullable":true}}'
        phase2 = b'{"fields":{"d/a":{"u":"kg"}}}'
        got = ArrowDigester.hash_schema(schema, include_metadata=True, **opts(flags))
        assert got == V + sha(tag(flags), phase1, phase2)

    def test_map_key_and_value_metadata_paths(self):
        key = pa.field("key", pa.utf8(), nullable=False, metadata={"role": "id"})
        value = pa.field("value", pa.int32(), metadata={"unit": "kg"})
        schema = pa.schema([pa.field("m", pa.map_(key, value), nullable=True)])
        entries = (
            b'{"data_type":{"Struct":[{"data_type":"LargeUtf8","name":"key","nullable":false},'
            b'{"data_type":"Int32","name":"value","nullable":true}]},"name":"entries","nullable":false}'
        )
        phase1 = b'{"m":{"data_type":{"Map":[' + entries + b',false]},"nullable":true}}'
        phase2 = b'{"fields":{"m//key":{"role":"id"},"m//value":{"unit":"kg"}}}'
        got = ArrowDigester.hash_schema(schema, include_metadata=True, nested_validity=True)
        assert got == V + sha(tag(0x04), phase1, phase2)

# ── Spec Section 9.8: invariants ───────────────────────────────────────


def _struct_xy(order: str) -> pa.StructArray:
    vals = {"x": pa.array([1, 2], pa.int32()), "y": pa.array([True, False])}
    return pa.StructArray.from_arrays(
        [vals[n] for n in order],
        fields=[pa.field(n, vals[n].type, nullable=False) for n in order],
    )


def _batch(cols: dict[str, pa.Array], nullable: bool = False) -> pa.RecordBatch:
    schema = pa.schema([pa.field(n, a.type, nullable=nullable) for n, a in cols.items()])
    return pa.record_batch(list(cols.values()), schema=schema)


class TestInvariants:
    def test_i1_all_off_equals_default(self):
        b = _batch({"b": pa.array([1, None], pa.int32()), "s": _struct_xy("yx")}, nullable=True)
        assert ArrowDigester.hash_record_batch(b, **opts(0)) == ArrowDigester.hash_record_batch(b)
        assert ArrowDigester.hash_schema(b.schema, **opts(0)) == ArrowDigester.hash_schema(b.schema)
        assert ArrowDigester.hash_array(b.column(1), **opts(0)) == ArrowDigester.hash_array(b.column(1))
        t = pa.Table.from_batches([b, b])
        assert ArrowDigester.hash_table(t, **opts(0)) == ArrowDigester.hash_table(t)

    @pytest.mark.parametrize("kind", ["schema", "batch", "array", "one_column_table"])
    def test_i2_all_flags_values_distinct(self, kind):
        b = _batch({"b": pa.array([1, None], pa.int32()), "s": _struct_xy("yx")}, nullable=True)
        if kind == "schema":
            hashes = [ArrowDigester.hash_schema(b.schema, **opts(f)) for f in ALL_FLAGS]
        elif kind == "batch":
            hashes = [batch_hash(b, f) for f in ALL_FLAGS]
        elif kind == "array":
            hashes = [ArrowDigester.hash_array(b.column(1), **opts(f)) for f in ALL_FLAGS]
        else:
            t = pa.table({"a": pa.array([1, 2], pa.int64())})
            hashes = [ArrowDigester.hash_table(t, **opts(f)) for f in ALL_FLAGS]
        assert len(set(hashes)) == 8

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_i3_column_permutation(self, flags):
        a, b = pa.array([1, 2], pa.int32()), pa.array(["x", None])
        h_ab = batch_hash(_batch({"a": a, "b": b}, nullable=True), flags)
        h_ba = batch_hash(_batch({"b": b, "a": a}, nullable=True), flags)
        s_ab = ArrowDigester.hash_schema(_batch({"a": a, "b": b}).schema, **opts(flags))
        s_ba = ArrowDigester.hash_schema(_batch({"b": b, "a": a}).schema, **opts(flags))
        if flags & 0x01:
            assert h_ab != h_ba and s_ab != s_ba
        else:
            assert h_ab == h_ba and s_ab == s_ba

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_i4_struct_field_permutation_depth_1(self, flags):
        h_xy = batch_hash(_batch({"s": _struct_xy("xy")}), flags)
        h_yx = batch_hash(_batch({"s": _struct_xy("yx")}), flags)
        a_xy = ArrowDigester.hash_array(_struct_xy("xy"), **opts(flags))
        a_yx = ArrowDigester.hash_array(_struct_xy("yx"), **opts(flags))
        if flags & 0x02:
            assert h_xy != h_yx and a_xy != a_yx
        else:
            assert h_xy == h_yx and a_xy == a_yx

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_i4_struct_field_permutation_nested_and_in_list(self, flags):
        def nested(order):
            inner = _struct_xy(order)
            return pa.StructArray.from_arrays(
                [pa.array([7, 8], pa.int64()), inner],
                fields=[pa.field("k", pa.int64(), nullable=False), pa.field("p", inner.type, nullable=False)],
            )

        def in_list(order):
            inner = _struct_xy(order)
            return pa.LargeListArray.from_arrays(pa.array([0, 1, 2], pa.int64()), inner)

        for make in (nested, in_list):
            h1 = batch_hash(_batch({"c": make("xy")}), flags)
            h2 = batch_hash(_batch({"c": make("yx")}), flags)
            assert (h1 != h2) == bool(flags & 0x02), make.__name__

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_i5_physical_equivalence(self, flags):
        pairs = [
            (pa.array(["a", None, "c"], pa.utf8()), pa.array(["a", None, "c"], pa.large_utf8())),
            (pa.array([b"a", None], pa.binary()), pa.array([b"a", None], pa.large_binary())),
            (pa.array([[1], None, [2, 3]], pa.list_(pa.int32())), pa.array([[1], None, [2, 3]], pa.large_list(pa.int32()))),
            (pa.array(["a", "b", "a"]).dictionary_encode(), pa.array(["a", "b", "a"], pa.large_utf8())),
        ]
        for small, large in pairs:
            assert ArrowDigester.hash_array(small, **opts(flags)) == ArrowDigester.hash_array(large, **opts(flags))
            assert batch_hash(_batch({"c": small}, nullable=True), flags) == batch_hash(
                _batch({"c": large}, nullable=True), flags
            )

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_i6_batch_split_independence(self, flags):
        s = pa.StructArray.from_arrays(
            [pa.array([1, None, 3, 4], pa.int32()), pa.array(["a", "b", None, "d"])],
            fields=[pa.field("z", pa.int32(), nullable=True), pa.field("a", pa.utf8(), nullable=True)],
            mask=pa.array([False, True, False, False]),
        )
        lst = pa.array([[[1], None], None, [[2, None]], []], pa.list_(pa.list_(pa.int32())))
        whole = _batch({"s": s, "l": lst}, nullable=True)
        d = ArrowDigester(whole.schema, **opts(flags))
        d.update(whole.slice(0, 1))
        d.update(whole.slice(1, 2))
        d.update(whole.slice(3, 1))
        assert d.finalize() == batch_hash(whole, flags)
        table = pa.Table.from_batches([whole.slice(0, 3), whole.slice(3, 1)])
        assert ArrowDigester.hash_table(table, **opts(flags)) == batch_hash(whole, flags)

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_i7_empty_metadata_invariant(self, flags):
        b = _batch({"b": pa.array([1], pa.int32()), "s": _struct_xy("yx").slice(0, 1)})
        assert ArrowDigester.hash_schema(b.schema, include_metadata=True, **opts(flags)) == (
            ArrowDigester.hash_schema(b.schema, include_metadata=False, **opts(flags))
        )
        assert batch_hash(b, flags, include_metadata=True) == batch_hash(b, flags, include_metadata=False)

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_metadata_still_counts_with_flags(self, flags):
        plain = pa.schema([pa.field("a", pa.int32())])
        meta = pa.schema([pa.field("a", pa.int32(), metadata={"k": "v"})])
        assert ArrowDigester.hash_schema(plain, include_metadata=True, **opts(flags)) != (
            ArrowDigester.hash_schema(meta, include_metadata=True, **opts(flags))
        )


# ── Spec Section 9.5: the four losses closed by nested_validity (I8) ──


def _nv_pairs():
    """Pairs of record batches that collide in the default layout (spec 9.5)."""
    a_null = pa.field("a", pa.int32(), nullable=True)

    def s_col(child, mask, field=a_null):
        return pa.StructArray.from_arrays([child], fields=[field], mask=pa.array(mask))

    # 1. null struct vs struct whose fields are all null
    p1 = (
        _batch({"s": s_col(pa.array([None, None], pa.int32()), [True, False])}, nullable=True),
        _batch({"s": s_col(pa.array([None, None], pa.int32()), [False, False])}, nullable=True),
    )
    # 2. nullable struct, non-nullable descendant: null rows vanish (3 rows vs 2 rows)
    a_req = pa.field("a", pa.int32(), nullable=False)
    p2 = (
        _batch({"s": s_col(pa.array([1, 9, 3], pa.int32()), [False, True, False], a_req)}, nullable=True),
        _batch({"s": s_col(pa.array([1, 3], pa.int32()), [False, False], a_req)}, nullable=True),
    )
    # 3. List<nullable List<Int32>>: outer list lengths lost
    lt = pa.list_(pa.field("item", pa.list_(pa.int32()), nullable=True))
    p3 = (
        _batch({"c": pa.array([[[1], [2]]], type=lt)}),
        _batch({"c": pa.array([[[1]], [[2]]], type=lt)}),
    )
    # 4. List<nullable Struct>: null element vs element with all-null fields
    st = pa.struct([a_null])
    ls = pa.list_(pa.field("item", st, nullable=True))
    p4 = (
        _batch({"c": pa.array([[None]], type=ls)}),
        _batch({"c": pa.array([[{"a": None}]], type=ls)}),
    )
    # 5. (9.5.1) non-nullable Boolean: padding bits look like false values
    bt = pa.field("b", pa.bool_(), nullable=False)
    p5 = (
        pa.record_batch([pa.array([True])], schema=pa.schema([bt])),
        pa.record_batch([pa.array([True, False, False])], schema=pa.schema([bt])),
    )
    return {
        "null_struct": p1,
        "vanishing_rows": p2,
        "list_of_nullable_list": p3,
        "list_of_nullable_struct": p4,
        "boolean_padding": p5,
    }


class TestNestedValidity:
    @pytest.mark.parametrize("case", list(_nv_pairs()))
    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_i8_losses(self, case, flags):
        left, right = _nv_pairs()[case]
        equal = batch_hash(left, flags) == batch_hash(right, flags)
        assert equal == (not flags & 0x04)

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_boolean_padding_hash_array(self, flags):
        one = ArrowDigester.hash_array(pa.array([True]), **opts(flags))
        two = ArrowDigester.hash_array(pa.array([True, False]), **opts(flags))
        assert (one == two) == (not flags & 0x04)

    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if f & 0x04])
    def test_boolean_batch_split_and_slices(self, flags):
        """Spec 9.5.1 / I8: Boolean leaves are split- and slice-independent with the third switch."""
        st = pa.struct([pa.field("v", pa.bool_(), nullable=False)])
        cols = {
            "b": pa.array([True, False, None, True, True, False, True, False, True]),
            "s": pa.array([{"v": i % 3 == 0} for i in range(9)], type=st),
            "l": pa.array([[True], [False, True], None, [], [True, True, False], [False], [True], [], [False]]),
        }
        whole = _batch(cols, nullable=True)
        ref = batch_hash(whole, flags)
        d = ArrowDigester(whole.schema, **opts(flags))
        for start, length in ((0, 3), (3, 1), (4, 5)):
            d.update(whole.slice(start, length))
        assert d.finalize() == ref
        d = ArrowDigester(whole.schema, **opts(flags))
        d.update(whole.slice(0, 0))
        d.update(whole)
        assert d.finalize() == ref
        # non-zero offset slices of an array equal the same values built fresh
        assert ArrowDigester.hash_array(cols["b"].slice(3, 5), **opts(flags)) == ArrowDigester.hash_array(
            pa.array(cols["b"].slice(3, 5).to_pylist()), **opts(flags)
        )

    @pytest.mark.parametrize("chunk", [1, 3, 8, 1 << 16])
    def test_boolean_bytes_hashed_in_bounded_chunks(self, chunk, monkeypatch):
        """Chunked hashing gives the spec 9.5.1 bytes across chunk boundaries, nulls and offsets."""
        import starfix.arrow_digester as ad

        monkeypatch.setattr(ad, "_BOOLEAN_CHUNK", chunk)
        values = [True, None, False, True, True, None, False, False, True, True, None]
        arr = pa.array(values).slice(1, 9)  # non-zero offset
        valid = [v for v in values[1:10] if v is not None]
        bits = sum(1 << i for i, v in enumerate(values[1:10]) if v is not None)
        expected = V + sha(
            tag(0x04), b'"Boolean"', u64(9) + bits.to_bytes(2, "little"), sha(bytes(int(v) for v in valid))
        )
        assert ArrowDigester.hash_array(arr, nested_validity=True) == expected
        no_nulls = pa.array([True, False, True, True, False, True, False]).slice(2, 5)
        expected = V + sha(tag(0x04), b'"Boolean"', sha(bytes([1, 1, 0, 1, 0])))
        assert ArrowDigester.hash_array(no_nulls, nested_validity=True) == expected

    def test_hash_array_nullable_struct_root_entry(self):
        """A nullable struct array passed to hash_array gets a validity-only entry at ""."""
        arr = pa.StructArray.from_arrays(
            [pa.array([1, 2], pa.int32())],
            fields=[pa.field("a", pa.int32(), nullable=False)],
            mask=pa.array([False, True]),
        )
        type_json = b'{"Struct":[{"data_type":"Int32","name":"a","nullable":false}]}'
        expected = V + sha(
            tag(0x04),
            type_json,
            u64(2) + b"\x01",  # entry "": struct validity [1,0]
            sha(i32(1)),  #       entry "a": non-nullable child, only the valid row
        )
        assert ArrowDigester.hash_array(arr, nested_validity=True) == expected

    def test_nested_nullable_structs_record_combined_validity(self):
        """Inner nullable struct bits = own AND ancestor (spec 9.5 traversal)."""
        inner = pa.StructArray.from_arrays(
            [pa.array([1, 2, 3], pa.int32())],
            fields=[pa.field("a", pa.int32(), nullable=False)],
            mask=pa.array([False, False, True]),  # inner validity [1,1,0]
        )
        outer = pa.StructArray.from_arrays(
            [inner],
            fields=[pa.field("t", inner.type, nullable=True)],
            mask=pa.array([True, False, False]),  # outer validity [0,1,1]
        )
        batch = _batch({"s": outer}, nullable=True)
        schema_digest = sha(
            tag(0x04),
            b'{"s":{"data_type":{"Struct":[{"data_type":{"Struct":[{"data_type":"Int32","name":"a",'
            b'"nullable":false}]},"name":"t","nullable":true}]},"nullable":true}}',
        )
        expected = V + sha(
            schema_digest,
            u64(3) + b"\x06",  # "s":   [0,1,1]
            u64(3) + b"\x02",  # "s/t": [1,1,0] AND [0,1,1] = [0,1,0]
            sha(i32(2)),  #     "s/t/a": only row 1
        )
        assert ArrowDigester.hash_record_batch(batch, nested_validity=True) == expected

    def test_non_nullable_struct_gets_no_entry(self):
        s = pa.StructArray.from_arrays(
            [pa.array([1, 2], pa.int32()), pa.array([3, 4], pa.int32())],
            fields=[pa.field("x", pa.int32(), nullable=False), pa.field("z", pa.int32(), nullable=False)],
        )
        batch = _batch({"s": s}, nullable=False)
        schema_digest = sha(
            tag(0x04),
            b'{"s":{"data_type":{"Struct":[{"data_type":"Int32","name":"x","nullable":false},'
            b'{"data_type":"Int32","name":"z","nullable":false}]},"nullable":false}}',
        )
        # no "s" entry: only "s/x" and "s/z"
        expected = V + sha(schema_digest, sha(i32(1, 2)), sha(i32(3, 4)))
        assert ArrowDigester.hash_record_batch(batch, nested_validity=True) == expected


# ── Spec Section 9.6: rejected schemas (ValueError) ───────────────────


def _rejected_schemas():
    i32_ = pa.int32()
    return {
        "duplicate_columns": pa.schema([pa.field("a", i32_), pa.field("a", pa.utf8())]),
        "duplicate_struct_children": pa.schema([
            pa.field("s", pa.struct([pa.field("x", i32_), pa.field("x", i32_)])),
        ]),
        "duplicate_children_nested_in_list": pa.schema([
            pa.field("l", pa.list_(pa.struct([pa.field("p", pa.struct([pa.field("q", i32_), pa.field("q", i32_)]))]))),
        ]),
        "slash_aliases_struct_child": pa.schema([
            pa.field("a/b", i32_),
            pa.field("a", pa.struct([pa.field("b", i32_)])),
        ]),
        "slash_aliases_list_element": pa.schema([
            pa.field("x", pa.list_(i32_)),
            pa.field("x/", i32_),
        ]),
        "slash_aliases_fixed_size_list_element": pa.schema([
            pa.field("x", pa.list_(i32_, 2)),
            pa.field("x/", i32_),
        ]),
        "duplicate_children_in_large_list_struct": pa.schema([
            pa.field("l", pa.large_list(pa.struct([pa.field("x", i32_), pa.field("x", i32_)]))),
        ]),
    }


class TestRejection:
    @pytest.mark.parametrize("case", list(_rejected_schemas()))
    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if f])
    def test_hash_schema_rejects(self, case, flags):
        schema = _rejected_schemas()[case]
        with pytest.raises(ValueError):
            ArrowDigester.hash_schema(schema, **opts(flags))
        with pytest.raises(ValueError):
            ArrowDigester(schema, **opts(flags))

    @pytest.mark.parametrize("case", list(_rejected_schemas()))
    def test_default_mode_does_not_reject_schema(self, case):
        ArrowDigester.hash_schema(_rejected_schemas()[case])
        ArrowDigester(_rejected_schemas()[case])

    def test_hash_record_batch_rejects_duplicate_columns(self):
        batch = pa.record_batch([pa.array([1]), pa.array([2])], names=["a", "a"])
        ArrowDigester.hash_record_batch(batch)  # default: accepted
        with pytest.raises(ValueError):
            ArrowDigester.hash_record_batch(batch, ordered_columns=True)

    def test_hash_table_rejects_duplicate_columns(self):
        table = pa.table([pa.array([1]), pa.array([2])], names=["a", "a"])
        with pytest.raises(ValueError):
            ArrowDigester.hash_table(table, nested_validity=True)

    def test_hash_array_rejects_duplicate_children(self):
        arr = pa.StructArray.from_arrays(
            [pa.array([1]), pa.array([2])],
            fields=[pa.field("x", pa.int64()), pa.field("x", pa.int64())],
        )
        ArrowDigester.hash_array(arr)  # default: accepted
        with pytest.raises(ValueError):
            ArrowDigester.hash_array(arr, ordered_struct_fields=True)

    def test_hash_array_rejects_child_named_empty_string(self):
        """claim("") for the root, then join("", "") == "" for the child (spec 9.6)."""
        arr = pa.StructArray.from_arrays([pa.array([1])], fields=[pa.field("", pa.int64())])
        with pytest.raises(ValueError):
            ArrowDigester.hash_array(arr, nested_validity=True)

    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if f])
    def test_slash_that_aliases_nothing_is_accepted(self, flags):
        schema = pa.schema([pa.field("km/h", pa.float64()), pa.field("km", pa.int32())])
        ArrowDigester.hash_schema(schema, **opts(flags))
        batch = pa.record_batch([pa.array([1.5]), pa.array([1], pa.int32())], schema=schema)
        ArrowDigester.hash_record_batch(batch, **opts(flags))

    def test_update_rejects_batch_with_rejected_schema(self):
        good = pa.schema([pa.field("a", pa.int64())])
        d = ArrowDigester(good, ordered_columns=True)
        bad = pa.record_batch([pa.array([1]), pa.array([2])], names=["a", "a"])
        with pytest.raises(ValueError):
            d.update(bad)


# ── Spec Section 9.7: streaming ───────────────────────────────────────


class TestStreaming:
    def _schemas(self):
        ab = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.utf8())])
        ba = pa.schema([pa.field("b", pa.utf8()), pa.field("a", pa.int32())])
        return ab, ba

    def _batch(self, schema):
        cols = {"a": pa.array([1, 2], pa.int32()), "b": pa.array(["x", None])}
        return pa.record_batch([cols[f.name] for f in schema], schema=schema)

    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if f & 0x01])
    def test_reordered_columns_rejected_with_ordered_columns(self, flags):
        ab, ba = self._schemas()
        d = ArrowDigester(ab, **opts(flags))
        d.update(self._batch(ab))
        with pytest.raises(ValueError, match="does not match"):
            d.update(self._batch(ba))

    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if not f & 0x01])
    def test_reordered_columns_accepted_without_ordered_columns(self, flags):
        ab, ba = self._schemas()
        d = ArrowDigester(ab, **opts(flags))
        d.update(self._batch(ab))
        d.update(self._batch(ba))
        ref = ArrowDigester(ab, **opts(flags))
        ref.update(self._batch(ab))
        ref.update(self._batch(ab))
        assert d.finalize() == ref.finalize()

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_reordered_struct_fields(self, flags):
        xy, yx = _batch({"s": _struct_xy("xy")}), _batch({"s": _struct_xy("yx")})
        d = ArrowDigester(xy.schema, **opts(flags))
        d.update(xy)
        if flags & 0x02:
            with pytest.raises(ValueError, match="does not match"):
                d.update(yx)
        else:
            d.update(yx)
            ref = ArrowDigester(xy.schema, **opts(flags))
            ref.update(xy)
            ref.update(xy)
            assert d.finalize() == ref.finalize()

    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if f])
    def test_different_type_rejected(self, flags):
        ab, _ = self._schemas()
        d = ArrowDigester(ab, **opts(flags))
        other = pa.record_batch([pa.array([1, 2], pa.int64()), pa.array(["x", None])], names=["a", "b"])
        with pytest.raises(ValueError, match="does not match"):
            d.update(other)

    @pytest.mark.parametrize("flags", [f for f in ALL_FLAGS if f])
    def test_metadata_mismatch_rejected_with_include_metadata(self, flags):
        """Spec 9.7 rule 2: with include_metadata the Phase 2 documents must be equal."""
        plain = pa.schema([pa.field("a", pa.int32())])
        meta = pa.schema([pa.field("a", pa.int32(), metadata={"k": "v"})])
        batch = pa.record_batch([pa.array([1], pa.int32())], schema=meta)
        ArrowDigester(plain, **opts(flags)).update(batch)  # metadata ignored without include_metadata
        d = ArrowDigester(plain, include_metadata=True, **opts(flags))
        with pytest.raises(ValueError, match="does not match"):
            d.update(batch)

    @pytest.mark.parametrize("flags", ALL_FLAGS)
    def test_equivalent_physical_types_accepted(self, flags):
        ab, _ = self._schemas()
        d = ArrowDigester(ab, **opts(flags))
        large = pa.record_batch(
            [pa.array([1, 2], pa.int32()), pa.array(["x", None], pa.large_utf8())],
            schema=pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.large_utf8())]),
        )
        d.update(large)
        assert d.finalize() == batch_hash(self._batch(ab), flags)


# ── API shape: keyword-only options with the Rust names ───────────────


class TestKeywordOnly:
    def test_constructor(self):
        schema = pa.schema([pa.field("a", pa.int32())])
        with pytest.raises(TypeError):
            ArrowDigester(schema, False, True)  # type: ignore[misc]
        ArrowDigester(schema, include_metadata=False, ordered_columns=True, ordered_struct_fields=True, nested_validity=True)

    def test_static_entry_points(self):
        schema = pa.schema([pa.field("a", pa.int32())])
        batch = pa.record_batch([pa.array([1], pa.int32())], schema=schema)
        table = pa.Table.from_batches([batch])
        arr = batch.column(0)
        with pytest.raises(TypeError):
            ArrowDigester.hash_schema(schema, False, True)  # type: ignore[misc]
        with pytest.raises(TypeError):
            ArrowDigester.hash_record_batch(batch, False, True)  # type: ignore[misc]
        with pytest.raises(TypeError):
            ArrowDigester.hash_table(table, False, True)  # type: ignore[misc]
        with pytest.raises(TypeError):
            ArrowDigester.hash_array(arr, True)  # type: ignore[misc]
        for name in _OPTION_FLAGS:
            ArrowDigester.hash_schema(schema, **{name: True})
            ArrowDigester.hash_record_batch(batch, **{name: True})
            ArrowDigester.hash_table(table, **{name: True})
            ArrowDigester.hash_array(arr, **{name: True})

    def test_hash_array_has_no_include_metadata(self):
        with pytest.raises(TypeError):
            ArrowDigester.hash_array(pa.array([1]), include_metadata=True)  # type: ignore[call-arg]

    def test_combinations_are_independent_switches(self):
        """All 8 combinations are accepted and each is reachable by keywords alone."""
        schema = pa.schema([pa.field("a", pa.int32())])
        for bits in itertools.product([False, True], repeat=3):
            ArrowDigester.hash_schema(schema, **dict(zip((OC, OSF, NV), bits, strict=True)))
