"""Every data type the format-2 spec hashes, on the installed pyarrow (ITL-898).

The supported pyarrow range starts at 19.0.0, whose compute kernels do not cover every
type (for example ``filter`` has no decimal32/decimal64 kernel there). This matrix runs
``hash_array``, ``hash_record_batch`` and the streaming digester for every supported leaf
type, with and without nulls, contiguous, sliced at a non-zero offset, split into chunks /
batches, dictionary-encoded (null keys, valid keys to null values, nested dictionaries),
and nested under list / large_list / struct / dictionary / fixed-size list (n = 0, 1, 2,
with a null slot and trailing child positions; fixed-size lists inside list / struct /
dictionary and lists / structs / fixed-size lists inside them). CI runs it on pyarrow
19.0.0, 20.0.0 and the latest pyarrow.

Expected values:

- where the Rust-generated golden file (``tests/golden/format2_v0.5.json``) has a
  ``hash_array`` vector for the same logical values, every variant must hash to it;
- otherwise no hash is invented here: every variant must hash without error and equal the
  hash of the plain, contiguous, single-chunk array of the same logical values. The
  dictionary-encoded variants are read through ``take`` rather than ``filter``, so a kernel
  missing on one path shows up as an error or as a mismatch between the two.

Inputs without a Rust golden vector are compared that way only; Rust vectors for them
belong in the Rust golden emitter (golden values are never produced on the Python side).

With ``STARFIX_EXPECT_PYARROW`` set (CI), the installed pyarrow must be that version, so a
job pinned to an old pyarrow cannot silently test another one.
"""

from __future__ import annotations

import base64
import io
import json
import os
from collections.abc import Callable
from functools import cache
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from starfix import ArrowDigester
from starfix._format2 import type_json

_FIXTURE_PATH = Path(__file__).parent / "golden" / "format2_v0.5.json"

_LEAF_TYPES: list[pa.DataType] = [
    pa.int8(), pa.int16(), pa.int32(), pa.int64(),
    pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64(),
    pa.float16(), pa.float32(), pa.float64(),
    pa.decimal32(7, 2), pa.decimal64(15, 3), pa.decimal128(30, 5), pa.decimal256(60, 10),
    pa.date32(), pa.date64(),
    pa.time32("s"), pa.time32("ms"), pa.time64("us"), pa.time64("ns"),
    pa.timestamp("s"), pa.timestamp("ms"), pa.timestamp("us"), pa.timestamp("ns"),
    pa.timestamp("us", "UTC"), pa.timestamp("ns", "America/Los_Angeles"),
    pa.duration("s"), pa.duration("ms"), pa.duration("us"), pa.duration("ns"),
    pa.bool_(),
    pa.binary(), pa.large_binary(), pa.string(), pa.large_string(),
    pa.binary(3), pa.binary(0),
]

_N = 11  # rows of a synthetic base array; not a multiple of 8, so slices are bit-unaligned
_NULLS = (1, 4, 5, 9)


# ---------------------------------------------------------------------------
# Base arrays: Rust golden vectors where they exist, synthetic otherwise
# ---------------------------------------------------------------------------

@cache
def _golden_leaf_arrays() -> dict[str, list[tuple[str, pa.Array, str]]]:
    """Plain (non-dictionary, non-nested) hash_array golden inputs, by canonical type JSON."""
    with _FIXTURE_PATH.open(encoding="utf-8") as f:
        vectors = json.load(f)["vectors"]
    out: dict[str, list[tuple[str, pa.Array, str]]] = {}
    for v in vectors:
        if v["method"] != "hash_array" or v["ordered_columns"] or v["ordered_struct_fields"]:
            continue
        batch = next(iter(pa.ipc.open_stream(io.BytesIO(base64.b64decode(v["ipc_b64"])))))
        col = batch.column(0)
        t = col.type
        if pa.types.is_dictionary(t) or pa.types.is_nested(t) or isinstance(t, pa.BaseExtensionType):
            continue
        out.setdefault(type_json(t, 0), []).append((v["id"], col, v["expected_hash"]))
    return out


def _validity(n: int, nulls: tuple[int, ...]) -> pa.Buffer | None:
    if not nulls:
        return None
    bits = bytearray((n + 7) // 8)
    for i in range(n):
        if i not in nulls:
            bits[i // 8] |= 1 << (i % 8)
    return pa.py_buffer(bytes(bits))


def _synthetic(t: pa.DataType, nulls: tuple[int, ...]) -> pa.Array:
    """_N values of type t; null slots hold non-zero bytes too (they must not be hashed)."""
    n = _N
    if pa.types.is_boolean(t):
        return pa.array([None if i in nulls else i % 3 == 0 for i in range(n)], t)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return pa.array([None if i in nulls else "é" * (i % 4) + str(i) for i in range(n)], t)
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return pa.array([None if i in nulls else bytes(range(i % 5)) + b"\xff" for i in range(n)], t)
    width = t.byte_width if pa.types.is_fixed_size_binary(t) else t.bit_width // 8
    # Small signed integers: valid for every fixed-width type (decimals within precision,
    # times within a day), stored as their little-endian bytes.
    data = b"".join((i * 11 - 50).to_bytes(width, "little", signed=True) if width else b"" for i in range(n))
    return pa.Array.from_buffers(t, n, [_validity(n, nulls), pa.py_buffer(data)], null_count=len(nulls))


def _bases(t: pa.DataType) -> list[tuple[str, pa.Array, str | None]]:
    """(label, array, Rust golden hash_array value or None) for the leaf type t."""
    bases: list[tuple[str, pa.Array, str | None]] = []
    for vid, col, expected in _golden_leaf_arrays().get(type_json(t, 0), []):
        bases.append((f"golden:{vid}", col if col.type == t else col.cast(t), expected))
    bases.append(("nulls", _synthetic(t, _NULLS), None))
    bases.append(("no-nulls", _synthetic(t, ()), None))
    return bases


# ---------------------------------------------------------------------------
# Equivalent constructions (built without compute kernels: concat, slice, from_arrays)
# ---------------------------------------------------------------------------

def _offset_by(a: pa.Array, k: int) -> pa.Array:
    """The same values at offset min(k, len(a)) of a longer array (a dictionary array is
    sliced through its keys: concatenating dictionaries would unify them)."""
    if isinstance(a, pa.DictionaryArray):
        return pa.DictionaryArray.from_arrays(_offset_by(a.indices, k), a.dictionary)
    prefix = a.slice(0, k)
    return pa.concat_arrays([prefix, a]).slice(len(prefix), len(a))


def _sliced(a: pa.Array) -> pa.Array:
    return _offset_by(a, len(a))


def _dict_null_keys(a: pa.Array) -> pa.DictionaryArray:
    """Null keys where a is null; the dictionary is a itself (its nulls are never referenced)."""
    keys = pa.array([i if a[i].is_valid else None for i in range(len(a))], pa.int32())
    return pa.DictionaryArray.from_arrays(keys, a)


def _dict_hidden_nulls(a: pa.Array) -> pa.DictionaryArray:
    """All keys valid; a null of a is a valid key pointing at a null dictionary value."""
    n = len(a)
    keys = pa.array([n + i for i in range(n)], pa.int16())
    return pa.DictionaryArray.from_arrays(keys, _sliced(pa.concat_arrays([a, a])) if n else a)


def _dict_nested(a: pa.Array) -> pa.DictionaryArray:
    """A dictionary whose dictionary is itself a (null-containing) dictionary."""
    inner = _dict_hidden_nulls(a)
    keys = pa.array(list(reversed(range(len(a)))), pa.int8())
    return pa.DictionaryArray.from_arrays(keys, _reversed_keys(inner))


def _reversed_keys(a: pa.DictionaryArray) -> pa.DictionaryArray:
    """a in reverse row order, by reversing its keys only (no kernel on the values)."""
    keys = pa.array([a.indices[i].as_py() for i in reversed(range(len(a)))], a.indices.type)
    return pa.DictionaryArray.from_arrays(keys, a.dictionary)


def _dict_sliced(a: pa.Array) -> pa.Array:
    return _sliced(_dict_hidden_nulls(a))


_LEAF_VARIANTS: dict[str, Callable[[pa.Array], pa.Array]] = {
    "sliced": _sliced,
    "offset3": lambda a: _offset_by(a, 3),
    "dict-null-keys": _dict_null_keys,
    "dict-hidden-nulls": _dict_hidden_nulls,
    "dict-nested": _dict_nested,
    "dict-sliced": _dict_sliced,
}


def _list_mask(rows: int) -> pa.Array:
    return pa.array([i == 1 for i in range(rows)], pa.bool_())


def _cuts(n: int) -> list[int]:
    return sorted({0, n // 3, (2 * n) // 3, n})


def _as_list(large: bool) -> Callable[[pa.Array], pa.Array]:
    """Lists over all items; list 1 is null and hides a non-empty item range."""
    cls, idx = (pa.LargeListArray, pa.int64()) if large else (pa.ListArray, pa.int32())

    def wrap(child: pa.Array) -> pa.Array:
        cuts = _cuts(len(child))
        return cls.from_arrays(pa.array(cuts, idx), child, mask=_list_mask(len(cuts) - 1))
    return wrap


def _as_struct(child: pa.Array) -> pa.Array:
    """struct<a: child, b: int8>; struct row 0 is null and hides its child values."""
    n = len(child)
    other = pa.array(list(range(n)), pa.int8())
    return pa.StructArray.from_arrays([child, other], names=["a", "b"], mask=pa.array([i == 0 for i in range(n)], pa.bool_()))


def _as_dict_of(wrap: Callable[[pa.Array], pa.Array]) -> Callable[[pa.Array], pa.Array]:
    """A dictionary whose values are wrap(child), keys in reverse order plus a null key."""
    def outer(child: pa.Array) -> pa.Array:
        values = wrap(child)
        m = len(values)
        keys = pa.array([None, *reversed(range(m))], pa.int32())
        return pa.DictionaryArray.from_arrays(keys, values)
    return outer


def _plain_dict_of(wrap: Callable[[pa.Array], pa.Array]) -> Callable[[pa.Array], pa.Array]:
    """The decoded form of _as_dict_of(wrap)(child), built without a dictionary: the same
    rows concatenated from slices (no compute kernel)."""
    def outer(child: pa.Array) -> pa.Array:
        values = wrap(child)
        m = len(values)
        rows = [pa.nulls(1, values.type), *(values.slice(i, 1) for i in reversed(range(m)))]
        return pa.concat_arrays(rows)
    return outer


def _fsl_slots(n: int, child: pa.Array) -> int:
    return len(child) if n == 0 else max(0, (len(child) - 1) // n)


def _as_fsl(n: int) -> Callable[[pa.Array], pa.Array]:
    """FixedSizeList<_, n> over the child, built from buffers: slot 1 is null and hides its n
    items, and at least one child position trails the last slot (n = 0: all of them)."""
    def wrap(child: pa.Array) -> pa.Array:
        slots = _fsl_slots(n, child)
        t = pa.list_(pa.field("item", child.type, nullable=True), n)
        return pa.Array.from_buffers(t, slots, [_validity(slots, (1,) if slots > 1 else ())], children=[child])
    return wrap


def _as_fsl_clean(n: int) -> Callable[[pa.Array], pa.Array]:
    """The logical value of _as_fsl(n)(child) with other physical content: nulls under the
    null slot and no trailing child positions (concatenated slices, no compute kernel)."""
    def wrap(child: pa.Array) -> pa.Array:
        slots = _fsl_slots(n, child)
        if slots > 1 and n:
            items = pa.concat_arrays([child.slice(0, n), pa.nulls(n, child.type), child.slice(2 * n, (slots - 2) * n)])
        else:
            items = child.slice(0, slots * n)
        t = pa.list_(pa.field("item", child.type, nullable=True), n)
        return pa.Array.from_buffers(t, slots, [_validity(slots, (1,) if slots > 1 else ())], children=[items])
    return wrap


_WRAPPERS: dict[str, tuple[Callable[[pa.Array], pa.Array], Callable[[pa.Array], pa.Array]]] = {
    # name: (wrapper, reference wrapper of the plain child)
    "leaf": (lambda a: a, lambda a: a),
    "list": (_as_list(False), _as_list(False)),
    "large_list": (_as_list(True), _as_list(True)),
    "struct": (_as_struct, _as_struct),
    "list<struct>": (lambda a: _as_list(False)(_as_struct(a)),) * 2,
    "struct<large_list>": (lambda a: _as_struct(_as_list(True)(a)),) * 2,
    "dict<struct>": (_as_dict_of(_as_struct), _plain_dict_of(_as_struct)),
    "dict<list>": (_as_dict_of(_as_list(False)), _plain_dict_of(_as_list(False))),
    "dict<large_list<struct>>": (
        _as_dict_of(lambda a: _as_list(True)(_as_struct(a))),
        _plain_dict_of(lambda a: _as_list(True)(_as_struct(a))),
    ),
    # FixedSizeList (ITL-927); each reference holds other content under null slots and no
    # trailing child positions, so hidden content is checked for every leaf type as well.
    "fsl[2]": (_as_fsl(2), _as_fsl_clean(2)),
    "fsl[1]": (_as_fsl(1), _as_fsl_clean(1)),
    "fsl[0]": (_as_fsl(0), _as_fsl_clean(0)),
    "fsl<struct>": (lambda a: _as_fsl(2)(_as_struct(a)), lambda a: _as_fsl_clean(2)(_as_struct(a))),
    "fsl<large_list>": (lambda a: _as_fsl(1)(_as_list(True)(a)), lambda a: _as_fsl_clean(1)(_as_list(True)(a))),
    "fsl<fsl>": (lambda a: _as_fsl(2)(_as_fsl(2)(a)), lambda a: _as_fsl_clean(2)(_as_fsl_clean(2)(a))),
    "list<fsl>": (lambda a: _as_list(False)(_as_fsl(2)(a)), lambda a: _as_list(False)(_as_fsl_clean(2)(a))),
    "struct<fsl>": (lambda a: _as_struct(_as_fsl(2)(a)), lambda a: _as_struct(_as_fsl_clean(2)(a))),
    "dict<fsl>": (_as_dict_of(_as_fsl(2)), _plain_dict_of(_as_fsl_clean(2))),
    "dict<fsl<struct>>": (
        _as_dict_of(lambda a: _as_fsl(1)(_as_struct(a))),
        _plain_dict_of(lambda a: _as_fsl_clean(1)(_as_struct(a))),
    ),
}


# ---------------------------------------------------------------------------
# Hashing through every entry point
# ---------------------------------------------------------------------------

def _hashes(arr: pa.Array | pa.ChunkedArray) -> dict[str, str]:
    """hash_array, the record hash (one batch, or a table of the chunks) and the record hash
    streamed through the digester in two batches per chunk."""
    schema = pa.schema([pa.field("c", arr.type, nullable=True)])
    if isinstance(arr, pa.ChunkedArray):
        chunks = arr.chunks
        records = ArrowDigester.hash_table(pa.Table.from_arrays([arr], schema=schema))
    else:
        chunks = [arr]
        records = ArrowDigester.hash_record_batch(pa.record_batch([arr], schema=schema))
    digester = ArrowDigester(schema)
    for chunk in chunks:
        k = len(chunk) // 2  # the second batch starts at a non-zero offset
        for part in (chunk.slice(0, k), chunk.slice(k)):
            digester.update(pa.record_batch([part], schema=schema))
    return {
        "hash_array": ArrowDigester.hash_array(arr).hex(),
        "records": records.hex(),
        "stream": digester.finalize().hex(),
    }


def _cases() -> list[Any]:
    cases = []
    for t in _LEAF_TYPES:
        for label, base, expected in _bases(t):
            for wrapper in _WRAPPERS:
                cases.append(pytest.param(t, base, expected if wrapper == "leaf" else None, wrapper,
                                          id=f"{t}-{label}-{wrapper}"))
    return cases


def test_installed_pyarrow_is_the_expected_one() -> None:
    expected = os.environ.get("STARFIX_EXPECT_PYARROW")
    if expected:
        assert pa.__version__ == expected


@pytest.mark.parametrize(("leaf_type", "base", "golden", "wrapper"), _cases())
def test_type_matrix(leaf_type: pa.DataType, base: pa.Array, golden: str | None, wrapper: str) -> None:
    wrap, reference_wrap = _WRAPPERS[wrapper]
    reference = reference_wrap(base)
    expected = _hashes(reference)
    assert expected["stream"] == expected["records"], "streamed batches differ from one batch"
    if golden is not None:
        assert expected["hash_array"] == golden, "plain array differs from the Rust golden vector"

    variants: dict[str, pa.Array | pa.ChunkedArray] = {"plain": wrap(base)}
    for name, make in _LEAF_VARIANTS.items():
        variants[f"child-{name}"] = wrap(make(base))
    wrapped = wrap(base)
    variants["outer-sliced"] = _sliced(wrapped)
    variants["outer-offset3"] = _offset_by(wrapped, 3)
    k = len(wrapped) // 3
    variants["chunked"] = pa.chunked_array([wrapped.slice(0, k), wrapped.slice(k)], type=wrapped.type)
    variants["chunked-sliced"] = pa.chunked_array(
        [_sliced(wrapped).slice(0, k), _offset_by(wrapped, 5).slice(k)], type=wrapped.type
    )

    for name, arr in variants.items():
        got = _hashes(arr)
        for entry_point, value in got.items():
            assert value == expected[entry_point], f"{leaf_type} {wrapper} {name}: {entry_point} differs"
