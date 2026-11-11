"""Hash format 2 (ITL-898), checked byte for byte against the specification.

Spec: ``nauticalab/starfix docs/byte-layout-spec.md`` (format 2, commit 041b8f1). Every
expected value is built here from the literal SHA-256 inputs in the spec, or is a final
hash the spec prints; never from the implementation under test. Cross-language values are
pinned separately by the Rust golden file once it lands.
"""

from __future__ import annotations

import hashlib
import struct

import pyarrow as pa
import pytest

from starfix import _format2 as f2

V = b"\x00\x00\x02"


def header(flags: int) -> bytes:
    return bytes.fromhex("00 73 74 61 72 66 69 78 00 00 02".replace(" ", "")) + bytes([flags])


def sha(*parts: bytes) -> bytes:
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


def u64(n: int) -> bytes:
    return struct.pack("<Q", n)


def i32(*vals: int) -> bytes:
    return b"".join(struct.pack("<i", v) for v in vals)


# ── Section 1: header ──────────────────────────────────────────────────


def test_header_bytes() -> None:
    assert f2.header(0x03) == header(0x03)
    assert len(f2.header(0)) == 12
    assert f2.VERSION_BYTES == V


# ── Section 2.1: canonical type JSON examples with printed hashes ───────


def test_map_example() -> None:
    value = pa.struct([pa.field("z", pa.int32(), nullable=False), pa.field("a", pa.int32(), nullable=False)])
    schema = pa.schema([pa.field("m", pa.map_(pa.utf8(), pa.field("value", value, nullable=True)), nullable=True)])
    json_ = (
        b'{"m":{"data_type":{"Map":{"key":{"data_type":"LargeUtf8","nullable":false},"keys_sorted":false,'
        b'"value":{"data_type":{"Struct":[{"data_type":"Int32","name":"a","nullable":false},'
        b'{"data_type":"Int32","name":"z","nullable":false}]},"nullable":true}}},"nullable":true}}'
    )
    expected = "0000021c34e18fc2130624f527774ee7992b42234963037109d81715bbacd180adcf1f"
    assert (V + sha(header(0), json_)).hex() == expected
    assert f2.hash_schema(schema).hex() == expected


def test_fixed_size_list_example() -> None:
    schema = pa.schema([pa.field("f", pa.list_(pa.field("item", pa.int32(), nullable=True), 2), nullable=False)])
    expected = "000002e04a62c05b99b0fce4b391ba58561f4a9df1e92319baaf3644496ad6e4209dcc"
    json_ = b'{"f":{"data_type":{"FixedSizeList":[{"data_type":"Int32","nullable":true},2]},"nullable":false}}'
    assert (V + sha(header(0), json_)).hex() == expected
    assert f2.hash_schema(schema).hex() == expected


def test_hash_array_non_ascii_example() -> None:
    arr = pa.StructArray.from_arrays([pa.array([1], pa.int32())], fields=[pa.field("é", pa.int32(), nullable=False)])
    expected = "000002cf1133b1070ab1b15f56e2013e4c4fe7a560d94d359c07868c42e4cfadfbe728"
    assert f2.hash_array(arr).hex() == expected


def test_json_text_rules() -> None:
    schema = pa.schema([pa.field("a\nb\x1b/\x7f\"\\é", pa.int8(), nullable=False)])
    name = '"a\\nb\\u001b/\x7f\\"\\\\é"'.encode()
    json_ = b"{" + name + b':{"data_type":"Int8","nullable":false}}'
    assert f2.hash_schema(schema) == V + sha(header(0), json_)


@pytest.mark.parametrize(
    "factory, form",
    [
        (lambda: pa.null(), '"Null"'),
        (lambda: pa.bool_(), '"Boolean"'),
        (lambda: pa.int8(), '"Int8"'),
        (lambda: pa.uint64(), '"UInt64"'),
        (lambda: pa.float16(), '"Float16"'),
        (lambda: pa.date64(), '"Date64"'),
        (lambda: pa.time32("s"), '{"Time32":"Second"}'),
        (lambda: pa.time64("ns"), '{"Time64":"Nanosecond"}'),
        (lambda: pa.timestamp("us", tz="UTC"), '{"Timestamp":["Microsecond","UTC"]}'),
        (lambda: pa.timestamp("s"), '{"Timestamp":["Second",null]}'),
        (lambda: pa.timestamp("ms", tz="+05:30"), '{"Timestamp":["Millisecond","+05:30"]}'),
        (lambda: pa.duration("ms"), '{"Duration":"Millisecond"}'),
        (lambda: pa.month_day_nano_interval(), '{"Interval":"MonthDayNano"}'),
        (lambda: pa.decimal128(38, 10), '{"Decimal128":[38,10]}'),
        (lambda: pa.decimal256(40, -3), '{"Decimal256":[40,-3]}'),
        (lambda: pa.binary(16), '{"FixedSizeBinary":16}'),
        (lambda: pa.utf8(), '"LargeUtf8"'),
        (lambda: pa.large_binary(), '"LargeBinary"'),
        (lambda: pa.dictionary(pa.int8(), pa.utf8(), ordered=True), '"LargeUtf8"'),
        (lambda: pa.list_(pa.field("element", pa.int8(), nullable=False)),
         '{"LargeList":{"data_type":"Int8","nullable":false}}'),
    ],
)
def test_primitive_and_canonical_forms(factory, form) -> None:
    schema = pa.schema([pa.field("c", factory(), nullable=True)])
    json_ = ('{"c":{"data_type":' + form + ',"nullable":true}}').encode()
    assert f2.hash_schema(schema) == V + sha(header(0), json_)


# ── Section 9: worked examples ─────────────────────────────────────────


def test_example_1_record_batch() -> None:
    schema = pa.schema([pa.field("a", pa.int32(), nullable=False), pa.field("b", pa.large_utf8(), nullable=True)])
    batch = pa.record_batch([pa.array([1, 2], pa.int32()), pa.array(["x", None], pa.large_utf8())], schema=schema)
    schema_digest = sha(
        header(0), b'{"a":{"data_type":"Int32","nullable":false},"b":{"data_type":"LargeUtf8","nullable":true}}'
    )
    final = sha(
        schema_digest,
        u64(2),
        sha(i32(1, 2)),
        u64(2) + b"\x01",
        sha(u64(1) + b"x"),
    )
    assert (V + final).hex() == "00000232bdf93232887698a4ec193cd978e6617c3d64a85be5d26c2853fcf82a42f3b6"
    assert f2.hash_record_batch(batch) == V + final


def test_example_2_list() -> None:
    t = pa.list_(pa.field("item", pa.int32(), nullable=True))
    schema = pa.schema([pa.field("c", t, nullable=True)])
    batch = pa.record_batch([pa.array([[1, None], None, []], t)], schema=schema)
    final = sha(
        f2.hash_schema(schema)[3:],
        u64(3),
        u64(3) + b"\x05",
        sha(u64(2) + u64(0)),
        u64(2) + b"\x01",
        sha(i32(1)),
    )
    assert f2.hash_record_batch(batch) == V + final


def test_example_3_nullable_struct() -> None:
    s = pa.StructArray.from_arrays(
        [pa.array([5, None, 1], pa.int32())],
        fields=[pa.field("a", pa.int32(), nullable=True)],
        mask=pa.array([True, False, False]),
    )
    schema = pa.schema([pa.field("s", s.type, nullable=True)])
    batch = pa.record_batch([s], schema=schema)
    final = sha(
        f2.hash_schema(schema)[3:],
        u64(3),
        u64(3) + b"\x06",
        u64(3) + b"\x04",
        sha(i32(1)),
    )
    assert f2.hash_record_batch(batch) == V + final


def test_example_4_list_of_nullable_struct() -> None:
    st = pa.struct([pa.field("a", pa.int32(), nullable=True)])
    t = pa.list_(pa.field("item", st, nullable=True))
    schema = pa.schema([pa.field("l", t, nullable=False)])
    batch = pa.record_batch([pa.array([[None, {"a": 4}]], t)], schema=schema)
    final = sha(
        f2.hash_schema(schema)[3:],
        u64(1),
        sha(u64(2)),  # "l": lengths
        u64(2) + b"\x02",  # "l/": validity [0,1]
        # "l//a" is nullable: validity [0,1] (struct null ANDed), values [4]
        u64(2) + b"\x02",
        sha(i32(4)),
    )
    assert f2.hash_record_batch(batch) == V + final


def test_example_4_as_printed_with_non_nullable_child() -> None:
    """The spec prints "l//a" without validity: the child a is then non-nullable."""
    st = pa.struct([pa.field("a", pa.int32(), nullable=False)])
    t = pa.list_(pa.field("item", st, nullable=True))
    schema = pa.schema([pa.field("l", t, nullable=False)])
    arr = pa.ListArray.from_arrays(
        pa.array([0, 2], pa.int32()),
        pa.StructArray.from_arrays([pa.array([0, 4], pa.int32())], fields=list(st), mask=pa.array([True, False])),
    ).cast(t)
    batch = pa.record_batch([arr], schema=schema)
    final = sha(f2.hash_schema(schema)[3:], u64(1), sha(u64(2)), u64(2) + b"\x02", sha(i32(4)))
    assert f2.hash_record_batch(batch) == V + final


def test_example_5_hash_array() -> None:
    expected = V + sha(header(0), b'"Int32"', u64(3), sha(i32(1, 2, 3)))
    assert expected.hex() == "0000024c470976f10386fad05c0ef8ac4d3f82c4fd656c3ea6255ef8a21180ce53597a"
    assert f2.hash_array(pa.array([1, 2, 3], pa.int32())) == expected
    boolean = V + sha(header(0), b'"Boolean"', u64(3), u64(3) + b"\x05", sha(b"\x01\x00"))
    assert f2.hash_array(pa.array([True, None, False])) == boolean


def test_example_6_options() -> None:
    schema = pa.schema([pa.field("b", pa.int32(), nullable=False), pa.field("a", pa.utf8(), nullable=True)])
    json_ = (
        b'[{"data_type":"Int32","name":"b","nullable":false},'
        b'{"data_type":"LargeUtf8","name":"a","nullable":true}]'
    )
    assert f2.hash_schema(schema, ordered_columns=True) == V + sha(header(0x01), json_)


def test_example_7_zero_columns() -> None:
    batch = pa.RecordBatch.from_struct_array(pa.array([{}, {}, {}], pa.struct([])))
    assert batch.num_columns == 0 and batch.num_rows == 3
    assert f2.hash_record_batch(batch) == V + sha(sha(header(0), b"{}"), u64(3))


# ── Sections 2.3, 2.4: metadata and names not hashed ───────────────────


def test_phase2_map_value_metadata_path() -> None:
    value = pa.field("value", pa.int32(), metadata={"unit": "kg"})
    schema = pa.schema([pa.field("m", pa.map_(pa.utf8(), value), nullable=True)])
    phase1 = (
        b'{"m":{"data_type":{"Map":{"key":{"data_type":"LargeUtf8","nullable":false},"keys_sorted":false,'
        b'"value":{"data_type":"Int32","nullable":true}}},"nullable":true}}'
    )
    assert f2.hash_schema(schema, include_metadata=True) == V + sha(
        header(0), phase1, b'{"fields":{"m//value":{"unit":"kg"}}}'
    )


def test_phase2_paths_and_schema_metadata() -> None:
    st = pa.struct([pa.field("x", pa.int8(), metadata={"k": "1"})])
    lt = pa.list_(pa.field("element", pa.int8(), metadata={"e": "2"}))
    schema = pa.schema(
        [pa.field("s", st, metadata={"c": "3"}), pa.field("l", lt), pa.field("d", pa.dictionary(pa.int8(), st))],
        metadata={"z": "9", "a": "0"},
    )
    expected_phase2 = (
        b'{"fields":{"d/x":{"k":"1"},"l/":{"e":"2"},"s":{"c":"3"},"s/x":{"k":"1"}},"schema":{"a":"0","z":"9"}}'
    )
    phase1 = f2.phase1_json(schema, 0).encode()
    assert f2.hash_schema(schema, include_metadata=True) == V + sha(header(0), phase1, expected_phase2)


def test_include_metadata_without_metadata_is_invariant() -> None:
    schema = pa.schema([pa.field("a", pa.int32())])
    assert f2.hash_schema(schema, include_metadata=True) == f2.hash_schema(schema)


@pytest.mark.parametrize(
    "left, right",
    [
        (pa.list_(pa.field("item", pa.int8())), pa.list_(pa.field("element", pa.int8()))),
        (pa.dictionary(pa.int8(), pa.utf8()), pa.dictionary(pa.int32(), pa.utf8(), ordered=True)),
        (pa.utf8(), pa.large_utf8()),
        (pa.binary(), pa.large_binary()),
        (pa.list_(pa.int8()), pa.large_list(pa.int8())),
    ],
)
def test_names_and_physical_variants_not_hashed(left, right) -> None:
    assert f2.hash_schema(pa.schema([pa.field("c", left)])) == f2.hash_schema(pa.schema([pa.field("c", right)]))


# ── Sections 3, 4: data layout rules ────────────────────────────────────


def test_non_nullable_child_of_null_struct_contributes_nothing() -> None:
    """Spec 3.2: elements whose (AND-ed) bit is 0 contribute no values (pending E2)."""
    s = pa.StructArray.from_arrays(
        [pa.array([1, 2, 3], pa.int32())],
        fields=[pa.field("a", pa.int32(), nullable=False)],
        mask=pa.array([False, True, False]),
    )
    schema = pa.schema([pa.field("s", s.type, nullable=True)])
    batch = pa.record_batch([s], schema=schema)
    final = sha(f2.hash_schema(schema)[3:], u64(3), u64(3) + b"\x05", sha(i32(1, 3)))
    assert f2.hash_record_batch(batch) == V + final


def test_row_count_distinguishes_empty_structs() -> None:
    def batch(n):
        return pa.RecordBatch.from_arrays([pa.array([{}] * n, pa.struct([]))], names=["s"])

    assert f2.hash_record_batch(batch(0)) != f2.hash_record_batch(batch(3))


def test_entry_order_is_bytewise_node_path_order() -> None:
    schema = pa.schema([pa.field("b", pa.int8(), nullable=False), pa.field("a", pa.int8(), nullable=False)])
    batch = pa.record_batch([pa.array([1], pa.int8()), pa.array([2], pa.int8())], schema=schema)
    final = sha(f2.hash_schema(schema)[3:], u64(1), sha(b"\x02"), sha(b"\x01"))
    assert f2.hash_record_batch(batch) == V + final


def test_leaf_value_bytes() -> None:
    cases = [
        (pa.array([1.5], pa.float64()), struct.pack("<d", 1.5)),
        (pa.array([-1], pa.int16()), struct.pack("<h", -1)),
        (pa.array([b"ab"], pa.binary()), u64(2) + b"ab"),
        (pa.array(["é"], pa.utf8()), u64(2) + "é".encode()),
        (pa.array([True, False], pa.bool_()), b"\x01\x00"),
        (pa.array([b"\x01\x02"], pa.binary(2)), b"\x01\x02"),
        (pa.array(["q", "p", "q"]).dictionary_encode(), u64(1) + b"q" + u64(1) + b"p" + u64(1) + b"q"),
        (pa.array([10, 20, 10], pa.int64()).dictionary_encode(), struct.pack("<qqq", 10, 20, 10)),
    ]
    for arr, value_bytes in cases:
        t = arr.type.value_type if pa.types.is_dictionary(arr.type) else arr.type
        type_json = f2.type_json(t, 0).encode()
        expected = V + sha(header(0), type_json, u64(len(arr)), sha(value_bytes))
        assert f2.hash_array(arr) == expected, arr.type


def test_hash_array_root_struct_nullable_and_list() -> None:
    s = pa.StructArray.from_arrays([pa.array([1, 2], pa.int8())], fields=[pa.field("x", pa.int8(), nullable=False)],
                                   mask=pa.array([False, True]))
    type_json = b'{"Struct":[{"data_type":"Int8","name":"x","nullable":false}]}'
    assert f2.hash_array(s) == V + sha(header(0), type_json, u64(2), u64(2) + b"\x01", sha(b"\x01"))
    lst = pa.array([[1, 2], [3]], pa.list_(pa.field("item", pa.int8(), nullable=False)))
    type_json = b'{"LargeList":{"data_type":"Int8","nullable":false}}'
    assert f2.hash_array(lst) == V + sha(header(0), type_json, u64(2), sha(u64(2) + u64(1)), sha(b"\x01\x02\x03"))


def test_hash_array_ordered_columns_changes_only_the_header() -> None:
    arr = pa.array([1], pa.int8())
    assert f2.hash_array(arr, ordered_columns=True) == V + sha(header(0x01), b'"Int8"', u64(1), sha(b"\x01"))


# ── Invariants (Section 11) ─────────────────────────────────────────────


def _mixed_batch() -> pa.RecordBatch:
    st = pa.struct([pa.field("z", pa.int32()), pa.field("a", pa.list_(pa.bool_()))])
    s = pa.array([{"z": 1, "a": [True]}, None, {"z": None, "a": None}, {"z": 4, "a": [False, None]}], st)
    return pa.record_batch(
        [pa.array(["x", None, "y", "z"]), s, pa.array([[[1]], None, [[2], None], []], pa.list_(pa.list_(pa.int64())))],
        names=["b", "s", "l"],
    )


def test_i1_order_options() -> None:
    batch = _mixed_batch()
    reordered = batch.select([2, 1, 0])
    assert f2.hash_record_batch(batch) == f2.hash_record_batch(reordered)
    assert f2.hash_record_batch(batch, ordered_columns=True) != f2.hash_record_batch(reordered, ordered_columns=True)
    st_xy = pa.StructArray.from_arrays([pa.array([1]), pa.array([2])], names=["x", "y"])
    st_yx = pa.StructArray.from_arrays([pa.array([2]), pa.array([1])], names=["y", "x"])
    assert f2.hash_array(st_xy) == f2.hash_array(st_yx)
    assert f2.hash_array(st_xy, ordered_struct_fields=True) != f2.hash_array(st_yx, ordered_struct_fields=True)


def test_i2_flag_values_never_coincide() -> None:
    batch = pa.record_batch([pa.array([1])], names=["a"])
    hashes = {
        f2.hash_record_batch(batch, ordered_columns=oc, ordered_struct_fields=osf)
        for oc in (False, True) for osf in (False, True)
    }
    assert len(hashes) == 4


def test_i4_batch_split_slicing_and_empty_batches() -> None:
    batch = _mixed_batch()
    whole = f2.hash_record_batch(batch)
    d = f2.Digester(batch.schema)
    d.update(batch.slice(0, 0))
    d.update(batch.slice(0, 1))
    d.update(batch.slice(1, 2))
    d.update(batch.slice(3))
    assert d.finalize() == whole
    assert f2.hash_table(pa.Table.from_batches([batch.slice(0, 3), batch.slice(3)])) == whole
    # adding a row changes the hash
    longer = pa.Table.from_batches([batch, batch.slice(0, 1)]).combine_chunks().to_batches()[0]
    assert f2.hash_record_batch(longer) != whole


def test_i3_physical_equivalence_in_data() -> None:
    small = pa.record_batch([pa.array([["a"], None], pa.list_(pa.utf8()))], names=["c"])
    large = pa.record_batch([pa.array([["a"], None], pa.large_list(pa.large_utf8()))], names=["c"])
    assert f2.hash_record_batch(small) == f2.hash_record_batch(large)


# ── Section 7: rejected inputs (every mode) ─────────────────────────────


@pytest.mark.parametrize(
    "schema",
    [
        pa.schema([pa.field("a", pa.int8()), pa.field("a", pa.int8())]),
        pa.schema([pa.field("s", pa.struct([pa.field("x", pa.int8()), pa.field("x", pa.int8())]))]),
        pa.schema([pa.field("a/b", pa.int8()), pa.field("a", pa.struct([pa.field("b", pa.int8())]))]),
        pa.schema([pa.field("x", pa.list_(pa.int8())), pa.field("x/", pa.int8())]),
        pa.schema([pa.field("m", pa.map_(pa.utf8(), pa.int8())), pa.field("m//key", pa.int8())]),
        pa.schema([pa.field("u", pa.dense_union([pa.field("a", pa.int8())]))]),
        pa.schema([pa.field("r", pa.run_end_encoded(pa.int32(), pa.int8()))]),
    ],
    ids=["dup_columns", "dup_children", "slash_alias", "list_alias", "map_alias", "union", "ree"],
)
def test_rejected_schemas(schema) -> None:
    with pytest.raises(ValueError):
        f2.hash_schema(schema)
    with pytest.raises(ValueError):
        f2.Digester(schema)


def test_list_view_rejected() -> None:
    with pytest.raises(ValueError):
        f2.hash_schema(pa.schema([pa.field("v", pa.list_view(pa.int8()))]))


def test_slash_aliasing_nothing_is_accepted() -> None:
    f2.hash_schema(pa.schema([pa.field("km/h", pa.float64()), pa.field("km", pa.int8())]))


def test_struct_child_paths_always_use_a_slash() -> None:
    """Spec 3.1: a struct child is p + "/" + name, also when p or name is empty."""
    # hash_array root struct child "" is the node "/" (distinct from the root "").
    arr = pa.StructArray.from_arrays([pa.array([1], pa.int64())], fields=[pa.field("", pa.int64(), nullable=False)])
    type_json = b'{"Struct":[{"data_type":"Int64","name":"","nullable":false}]}'
    assert f2.hash_array(arr) == V + sha(header(0), type_json, u64(1), sha(struct.pack("<q", 1)))
    # A struct column "" with child "a" (node "/a") next to a column "/a" is ambiguous ...
    st = pa.struct([pa.field("a", pa.int8())])
    with pytest.raises(ValueError):
        f2.hash_schema(pa.schema([pa.field("", st), pa.field("/a", pa.int8())]))
    # ... while a column "a" next to it is not.
    f2.hash_schema(pa.schema([pa.field("", st), pa.field("a", pa.int8())]))


def test_hash_array_root_struct_child_metadata_free_paths_sort_after_root() -> None:
    """Entries of a root struct: "" (validity) then "/x" (byte order)."""
    s = pa.StructArray.from_arrays([pa.array([1, 2], pa.int8())], fields=[pa.field("x", pa.int8(), nullable=True)],
                                   mask=pa.array([False, True]))
    type_json = b'{"Struct":[{"data_type":"Int8","name":"x","nullable":true}]}'
    expected = V + sha(header(0), type_json, u64(2), u64(2) + b"\x01", u64(2) + b"\x01", sha(b"\x01"))
    assert f2.hash_array(s) == expected


# ── Section 5: per-batch schema check ──────────────────────────────────


def test_streaming_check() -> None:
    schema = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.utf8())])
    reordered = pa.record_batch([pa.array(["x"]), pa.array([1], pa.int32())], names=["b", "a"])
    f2.Digester(schema).update(reordered)
    with pytest.raises(ValueError, match="does not match"):
        f2.Digester(schema, ordered_columns=True).update(reordered)
    with pytest.raises(ValueError, match="does not match"):
        f2.Digester(schema).update(pa.record_batch([pa.array([1], pa.int64()), pa.array(["x"])], names=["a", "b"]))
    large = pa.record_batch([pa.array([1], pa.int32()), pa.array(["x"], pa.large_utf8())], names=["a", "b"])
    f2.Digester(schema).update(large)
    meta = pa.schema([pa.field("a", pa.int32(), metadata={"k": "v"}), pa.field("b", pa.utf8())])
    batch = pa.record_batch([pa.array([1], pa.int32()), pa.array(["x"])], schema=meta)
    f2.Digester(schema).update(batch)  # metadata ignored without include_metadata
    with pytest.raises(ValueError, match="does not match"):
        f2.Digester(schema, include_metadata=True).update(batch)


# ── Section 8: unsupported data ─────────────────────────────────────────


@pytest.mark.parametrize(
    "t",
    [pa.null(), pa.list_(pa.int8(), 2), pa.map_(pa.utf8(), pa.int8()), pa.month_day_nano_interval(),
     pa.struct([pa.field("n", pa.null())]), pa.list_(pa.null())],
    ids=["null", "fixed_size_list", "map", "interval", "nested_null", "list_of_null"],
)
def test_unsupported_data_rejected_whatever_the_row_count(t) -> None:
    """Spec 7 item 3: rejected before any data is hashed, also with zero rows / no batches."""
    schema = pa.schema([pa.field("c", t)])
    f2.hash_schema(schema)  # schemas of these types hash
    with pytest.raises(ValueError):
        f2.Digester(schema)
    empty = pa.RecordBatch.from_arrays([pa.array([], t)], schema=schema)
    with pytest.raises(ValueError):
        f2.hash_record_batch(empty)
    with pytest.raises(ValueError):
        f2.hash_array(pa.array([], t))


# ── Section 2.3: extension types (E1 pending for pyarrow's C++ built-in types) ──


class _Point(pa.ExtensionType):
    def __init__(self, unit: str = "m") -> None:
        self.unit = unit
        super().__init__(pa.struct([pa.field("x", pa.float64())]), "starfix.test.point")

    def __arrow_ext_serialize__(self) -> bytes:
        return ('{"unit":"%s"}' % self.unit).encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        import json as _json

        return cls(_json.loads(serialized)["unit"])


@pytest.fixture
def point_type():
    pa.register_extension_type(_Point())
    yield _Point
    pa.unregister_extension_type("starfix.test.point")


def _ipc_roundtrip(schema: pa.Schema, batches=()) -> tuple[pa.Schema, list]:
    import io

    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, schema) as w:
        for b in batches:
            w.write_batch(b)
    reader = pa.ipc.open_stream(io.BytesIO(sink.getvalue()))
    return reader.schema, list(reader)


def test_registered_python_extension_type(point_type) -> None:
    storage = pa.struct([pa.field("x", pa.float64())])
    schema = pa.schema([pa.field("p", point_type("km"), metadata={"other": "x"})])
    phase1 = b'{"p":{"data_type":{"Struct":[{"data_type":"Float64","name":"x","nullable":true}]},"nullable":true}}'
    phase2 = b'{"fields":{"p":{"ARROW:extension:metadata":"{\\"unit\\":\\"km\\"}","ARROW:extension:name":"starfix.test.point","other":"x"}}}'
    assert f2.hash_schema(schema, include_metadata=True) == V + sha(header(0), phase1, phase2)
    # Without include_metadata an extension type hashes exactly like its storage type.
    assert f2.hash_schema(schema) == f2.hash_schema(pa.schema([pa.field("p", storage)]))
    # Data: the storage array is hashed.
    storage_arr = pa.array([{"x": 1.0}, None], storage)
    ext_arr = pa.ExtensionArray.from_storage(point_type("km"), storage_arr)
    plain = pa.record_batch([storage_arr], names=["p"])
    ext = pa.record_batch([ext_arr], names=["p"])
    assert f2.hash_record_batch(ext) == f2.hash_record_batch(plain)
    assert f2.hash_array(ext_arr) == f2.hash_array(storage_arr)


def test_registered_python_extension_type_after_ipc(point_type) -> None:
    """pyarrow hides the two keys of a registered type; they are re-added from the type."""
    stored = pa.schema([pa.field("p", pa.struct([pa.field("x", pa.float64())]), metadata={
        "ARROW:extension:name": "starfix.test.point", "ARROW:extension:metadata": '{"unit":"km"}'})])
    seen, _ = _ipc_roundtrip(stored)
    assert isinstance(seen.field(0).type, point_type)
    assert f2.hash_schema(seen, include_metadata=True) == f2.hash_schema(stored, include_metadata=True)


def test_unregistered_extension_keys_hashed_verbatim() -> None:
    schema = pa.schema([pa.field("q", pa.int32(), metadata={
        "ARROW:extension:name": "other.thing", "ARROW:extension:metadata": "v1"})])
    seen, _ = _ipc_roundtrip(schema)
    phase2 = b'{"fields":{"q":{"ARROW:extension:metadata":"v1","ARROW:extension:name":"other.thing"}}}'
    assert f2.hash_schema(seen, include_metadata=True) == V + sha(header(0), b'{"q":{"data_type":"Int32","nullable":true}}', phase2)


def test_extension_name_without_metadata_key_is_hashed_as_empty_metadata() -> None:
    """Spec 2.3: ARROW:extension:name present and :metadata absent -> hashed as "" (E1)."""
    name_only = pa.schema([pa.field("q", pa.int32(), metadata={"ARROW:extension:name": "other.thing"})])
    empty = pa.schema([pa.field("q", pa.int32(), metadata={
        "ARROW:extension:name": "other.thing", "ARROW:extension:metadata": ""})])
    phase2 = b'{"fields":{"q":{"ARROW:extension:metadata":"","ARROW:extension:name":"other.thing"}}}'
    expected = V + sha(header(0), b'{"q":{"data_type":"Int32","nullable":true}}', phase2)
    assert f2.hash_schema(name_only, include_metadata=True) == expected
    assert f2.hash_schema(empty, include_metadata=True) == expected
    seen, _ = _ipc_roundtrip(name_only)
    assert f2.hash_schema(seen, include_metadata=True) == expected


def test_builtin_uuid_extension_with_canonical_metadata() -> None:
    """arrow.uuid stored with an empty metadata value: exact (E1 covers other stored forms)."""
    stored = pa.schema([pa.field("u", pa.binary(16), metadata={
        "ARROW:extension:name": "arrow.uuid", "ARROW:extension:metadata": ""})])
    batch = pa.record_batch([pa.array([b"\x01" * 16, None], pa.binary(16))], schema=stored)
    seen, batches = _ipc_roundtrip(stored, [batch])
    assert f2.hash_schema(seen, include_metadata=True) == f2.hash_schema(stored, include_metadata=True)
    assert f2.hash_record_batch(batches[0]) == f2.hash_record_batch(batch)


# ── Review items from the Rust PR (dictionary nullability, offsets) ─────


def test_hash_array_dictionary_nullability_counts_resolved_nulls() -> None:
    """A valid key pointing at a null dictionary value is a null element."""
    arr = pa.DictionaryArray.from_arrays(pa.array([0, 1], pa.int8()), pa.array(["a", None]))
    assert arr.null_count == 0  # pyarrow counts index nulls only
    expected = V + sha(header(0), b'"LargeUtf8"', u64(2), u64(2) + b"\x01", sha(u64(1) + b"a"))
    assert f2.hash_array(arr) == expected


def _offset_cases():
    n = 200
    bools = [None if i % 5 == 0 else i % 3 == 0 for i in range(n)]
    ints = [None if i % 7 == 0 else i for i in range(n)]
    strs = [None if i % 4 == 0 else "s" * (i % 5) for i in range(n)]
    mask = [i % 6 == 0 for i in range(n)]
    st = pa.StructArray.from_arrays(
        [pa.array(bools, pa.bool_()), pa.array(ints, pa.int32()), pa.array(strs)],
        names=["b", "i", "s"], mask=pa.array(mask),
    )
    lst = pa.array([None if i % 9 == 0 else bools[i:i + i % 4] for i in range(n)], pa.list_(pa.bool_()))
    return st, lst


@pytest.mark.parametrize("offset", [7, 8, 9, 16, 63, 64, 65])
def test_slices_at_bit_and_byte_boundaries(offset: int) -> None:
    """Leaves under nullable containers at array offsets around byte/word boundaries."""
    st, lst = _offset_cases()
    for arr in (st, lst):
        sliced = arr.slice(offset, 50)
        fresh = pa.array(sliced.to_pylist(), sliced.type)
        assert sliced.offset == offset and fresh.offset == 0
        assert f2.hash_array(sliced) == f2.hash_array(fresh), (arr.type, offset)
        b_sliced = pa.record_batch([sliced], names=["c"])
        b_fresh = pa.record_batch([fresh], names=["c"])
        assert f2.hash_record_batch(b_sliced) == f2.hash_record_batch(b_fresh), (arr.type, offset)


# ── Copilot review round (PR #9) ─────────────────────────────────────────


def test_unmasked_null_in_non_nullable_column_rejected() -> None:
    schema = pa.schema([pa.field("a", pa.int32(), nullable=False)])
    batch = pa.record_batch([pa.array([1, None], pa.int32())], schema=schema)
    with pytest.raises(ValueError, match="Non-nullable"):
        f2.hash_record_batch(batch)


def test_unmasked_null_in_non_nullable_struct_child_rejected() -> None:
    st = pa.struct([pa.field("x", pa.int32(), nullable=False)])
    s = pa.StructArray.from_arrays([pa.array([1, None], pa.int32())], fields=list(st))  # struct valid, child null
    with pytest.raises(ValueError, match="Non-nullable"):
        f2.hash_record_batch(pa.record_batch([s], schema=pa.schema([pa.field("s", st, nullable=True)])))


def test_dictionary_index_to_null_value_in_non_nullable_column_rejected() -> None:
    arr = pa.DictionaryArray.from_arrays(pa.array([0, 1], pa.int8()), pa.array(["a", None]))
    schema = pa.schema([pa.field("d", arr.type, nullable=False)])
    with pytest.raises(ValueError, match="Non-nullable"):
        f2.hash_record_batch(pa.record_batch([arr], schema=schema))


def test_child_null_masked_by_null_parent_is_accepted() -> None:
    st = pa.struct([pa.field("x", pa.int32(), nullable=False)])
    s = pa.StructArray.from_arrays([pa.array([1, None], pa.int32())], fields=list(st), mask=pa.array([False, True]))
    schema = pa.schema([pa.field("s", st, nullable=True)])
    final = sha(f2.hash_schema(schema)[3:], u64(2), u64(2) + b"\x01", sha(i32(1)))
    assert f2.hash_record_batch(pa.record_batch([s], schema=schema)) == V + final


def test_rejected_batch_leaves_the_digester_unchanged() -> None:
    """A batch rejected while it is traversed must not change the digester's state."""
    schema = pa.schema([pa.field("a", pa.int32(), nullable=False), pa.field("b", pa.int32(), nullable=False)])
    good = pa.record_batch([pa.array([1], pa.int32()), pa.array([2], pa.int32())], schema=schema)
    bad = pa.record_batch([pa.array([7], pa.int32()), pa.array([None], pa.int32())], schema=schema)
    d = f2.Digester(schema)
    d.update(good)
    with pytest.raises(ValueError):
        d.update(bad)
    assert d.finalize() == f2.hash_record_batch(good)


def test_dictionary_of_extension_metadata_is_hashed(point_type) -> None:
    km = pa.schema([pa.field("p", pa.dictionary(pa.int32(), point_type("km")))])
    m = pa.schema([pa.field("p", pa.dictionary(pa.int32(), point_type("m")))])
    assert f2.hash_schema(km, include_metadata=True) != f2.hash_schema(m, include_metadata=True)
    phase2 = f2.phase2_json(km)
    assert '"ARROW:extension:name":"starfix.test.point"' in phase2
    assert '"ARROW:extension:metadata":"{\\"unit\\":\\"km\\"}"' in phase2


def test_binary_values_hashed_with_a_byte_budget(monkeypatch) -> None:
    """Large values are not copied into one joined buffer; the bytes are unchanged."""
    monkeypatch.setattr(f2, "_BYTE_BUDGET", 10)
    values = [b"x" * 25, b"", b"abc", None, b"y" * 9, b"z" * 11]
    arr = pa.array(values, pa.binary())
    expected = b"".join(u64(len(v)) + v for v in values if v is not None)
    valid = u64(6) + bytes([0b110111])
    assert f2.hash_array(arr) == V + sha(header(0), b'"LargeBinary"', u64(6), valid, sha(expected))


def test_rejected_batch_restores_partial_validity_bytes() -> None:
    """Rollback restores validity bits that end mid-byte, and the hash states."""
    schema = pa.schema([pa.field("a", pa.int32(), nullable=True), pa.field("b", pa.utf8(), nullable=False)])
    good = pa.record_batch([pa.array([1, None, 3], pa.int32()), pa.array(["x", "y", "z"])], schema=schema)
    bad = pa.record_batch([pa.array([None] * 7 + [4], pa.int32()), pa.array(["p"] * 7 + [None])], schema=schema)
    d = f2.Digester(schema)
    d.update(good)
    with pytest.raises(ValueError):
        d.update(bad)
    d.update(good)
    assert d.finalize() == f2.hash_table(pa.Table.from_batches([good, good]))


@pytest.mark.parametrize("budget", [1, 7, 64, 1 << 20])
def test_nullable_leaves_hashed_in_bounded_chunks(budget, monkeypatch) -> None:
    """Chunked processing of nullable leaves gives the spec bytes at any chunk size."""
    monkeypatch.setattr(f2, "_BYTE_BUDGET", budget)
    monkeypatch.setattr(f2, "_CHUNK", max(1, budget // 8))
    strs = ["a", None, "bcd", "", None, "é" * 3, "z"]
    ints = [1, None, 3, None, 5, 6, 7]
    bools = [True, None, False, True, None, False, True]
    fsb = [b"\x01\x02", None, b"\x03\x04", b"\x05\x06", None, b"\x07\x08", b"\x09\x0a"]
    cases = [
        (pa.array(strs).slice(1), b"".join(u64(len(s.encode())) + s.encode() for s in strs[1:] if s is not None)),
        (pa.array(ints, pa.int32()).slice(1), i32(*[v for v in ints[1:] if v is not None])),
        (pa.array(bools).slice(1), bytes(int(v) for v in bools[1:] if v is not None)),
        (pa.array(fsb, pa.binary(2)).slice(1), b"".join(v for v in fsb[1:] if v is not None)),
    ]
    for arr, value_bytes in cases:
        valid = [v is not None for v in arr.to_pylist()]
        bits = sum(1 << i for i, ok in enumerate(valid) if ok)
        validity = u64(len(arr)) + bits.to_bytes((len(arr) + 7) // 8, "little")
        expected = V + sha(header(0), f2.type_json(arr.type, 0).encode(), u64(len(arr)), validity, sha(value_bytes))
        assert f2.hash_array(arr) == expected, (arr.type, budget)


def test_zero_width_fixed_size_binary() -> None:
    """FixedSizeBinary(0) values contribute no bytes; nulls are still recorded."""
    arr = pa.array([b"", None, b""], type=pa.binary(0))
    expected = V + sha(header(0), b'{"FixedSizeBinary":0}', u64(3), u64(3) + b"\x05", sha(b""))
    assert f2.hash_array(arr) == expected
    st = pa.StructArray.from_arrays([pa.array([b"", b""], pa.binary(0))], fields=[pa.field("z", pa.binary(0), nullable=False)],
                                    mask=pa.array([False, True]))
    f2.hash_record_batch(pa.record_batch([st], names=["s"]))  # masked by a nullable struct


# ── Inputs from the Rust review (ITL-898) ───────────────────────────────


def test_dictionary_of_list_equals_plain_list() -> None:
    """A dictionary whose values are a List (32-bit offsets) hashes as the plain list."""
    lt = pa.list_(pa.int32())
    dict_arr = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0, 2, None], pa.int8()), pa.array([[1, 2], [3], None], lt))
    plain = pa.array([[1, 2], [3], [1, 2], None, None], lt)
    assert f2.hash_array(dict_arr) == f2.hash_array(plain)
    assert f2.hash_record_batch(pa.record_batch([dict_arr], names=["c"])) == f2.hash_record_batch(
        pa.record_batch([plain], names=["c"])
    )
    s_dict = pa.StructArray.from_arrays([dict_arr], names=["l"])
    s_plain = pa.StructArray.from_arrays([plain], names=["l"])
    assert f2.hash_record_batch(pa.record_batch([s_dict], names=["s"])) == f2.hash_record_batch(
        pa.record_batch([s_plain], names=["s"])
    )


def test_list_non_nullable_items_with_hidden_child_nulls() -> None:
    """Child nulls under a null list slot or outside the offsets are not elements (accepted)."""
    t = pa.list_(pa.field("item", pa.int32(), nullable=False))
    schema = pa.schema([pa.field("l", t)])
    child = pa.array([1, None, 2, None], pa.int32())
    under_null_slot = pa.ListArray.from_arrays(
        pa.array([0, 1, 2, 3], pa.int32()), child.slice(0, 3), mask=pa.array([False, True, False])
    ).cast(t)
    clean = pa.ListArray.from_arrays(
        pa.array([0, 1, 1, 2], pa.int32()), pa.array([1, 2], pa.int32()), mask=pa.array([False, True, False])
    ).cast(t)
    assert f2.hash_record_batch(pa.record_batch([under_null_slot], schema=schema)) == f2.hash_record_batch(
        pa.record_batch([clean], schema=schema)
    )
    outside_offsets = pa.ListArray.from_arrays(pa.array([0, 1, 1], pa.int32()), child).cast(t)
    assert f2.hash_record_batch(pa.record_batch([outside_offsets], schema=schema)) == f2.hash_record_batch(
        pa.record_batch([pa.array([[1], []], t)], schema=schema)
    )
    visible = pa.ListArray.from_arrays(pa.array([0, 2], pa.int32()), pa.array([1, None], pa.int32())).cast(t)
    with pytest.raises(ValueError, match="Non-nullable"):
        f2.hash_array(visible)
