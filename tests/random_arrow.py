"""Seeded random Arrow schemas and data for differential tests (ITL-1177).

Used by ``tests/test_prepared_schema.py`` and ``scripts/release_differential.py`` (which runs it
against the released starfix in an isolated environment), so it depends on pyarrow only.
Everything is built from ``random.Random(seed)``: the same seed gives the same inputs in any
process. Covers every type family whose data hashes: fixed-width and variable-length leaves,
booleans, dictionaries (of leaves and of structs), list / large_list, fixed-size lists, structs,
extension types (canonical and Python-defined), field metadata at every depth, schema metadata,
nulls, sliced arrays, empty batches and multi-batch tables.
"""

from __future__ import annotations

import random
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc


class UnitType(pa.ExtensionType):
    """A Python-defined extension type with a parameter carried only in its serialization."""

    def __init__(self, unit: str = "m") -> None:
        self.unit = unit
        super().__init__(pa.int64(), "starfix.test.unit")

    def __arrow_ext_serialize__(self) -> bytes:
        return self.unit.encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> UnitType:
        return cls(serialized.decode())


LEAVES: list[pa.DataType] = [
    pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint32(), pa.uint64(),
    pa.float16(), pa.float32(), pa.float64(), pa.decimal128(10, 2), pa.decimal256(40, 5),
    pa.date32(), pa.date64(), pa.time32("ms"), pa.time64("ns"),
    pa.timestamp("us"), pa.timestamp("ms", tz="UTC"), pa.duration("s"),
    pa.binary(4), pa.binary(0), pa.string(), pa.large_string(), pa.binary(), pa.large_binary(), pa.bool_(),
]


def _extension_types() -> list[pa.DataType]:
    types: list[pa.DataType] = [UnitType("m"), UnitType("s"), pa.fixed_shape_tensor(pa.float32(), [2, 2])]
    for name in ("uuid", "json_", "bool8"):
        factory = getattr(pa, name, None)
        if factory is not None:
            types.append(factory())
    return types


EXTENSIONS = _extension_types()
_NAMES = ["a", "b", "c", "d", "é"]


def random_type(rng: random.Random, depth: int = 0) -> pa.DataType:
    kind = rng.random()
    if depth >= 3 or kind < 0.35:
        return rng.choice(LEAVES)
    if kind < 0.45:
        value = rng.choice([pa.string(), pa.int64(), pa.float32()])
        return pa.dictionary(rng.choice([pa.int8(), pa.int32()]), value)
    if kind < 0.5:
        return pa.dictionary(pa.int32(), pa.struct([random_field(rng, n, depth + 1) for n in ("x", "y")]))
    if kind < 0.6:
        return rng.choice(EXTENSIONS)
    if kind < 0.75:
        element = random_field(rng, "item", depth + 1)
        return pa.large_list(element) if rng.random() < 0.3 else pa.list_(element)
    if kind < 0.82:
        return pa.list_(random_field(rng, "item", depth + 1), rng.choice([0, 1, 2, 3]))
    names = rng.sample(_NAMES, rng.randint(0, 3))
    return pa.struct([random_field(rng, n, depth + 1) for n in names])


def random_metadata(rng: random.Random) -> dict[str, str] | None:
    if rng.random() < 0.6:
        return None
    return {rng.choice(["unit", "z", "é", "a"]): rng.choice(["kg", "", "v1", "ü"]) for _ in range(rng.randint(1, 2))}


def random_field(rng: random.Random, name: str, depth: int = 0) -> pa.Field:
    return pa.field(name, random_type(rng, depth), nullable=rng.random() < 0.6, metadata=random_metadata(rng))


def random_schema(rng: random.Random) -> pa.Schema:
    names = rng.sample(_NAMES, rng.randint(0, 4))
    return pa.schema([random_field(rng, n) for n in names], metadata=random_metadata(rng))


def _leaf_values(t: pa.DataType, n: int, nullable: bool, rng: random.Random) -> pa.Array:
    def value():
        if pa.types.is_boolean(t):
            return rng.random() < 0.5
        if pa.types.is_floating(t):
            return rng.uniform(-1e3, 1e3)
        if pa.types.is_decimal(t):
            return Decimal(rng.randint(-99999, 99999)) / 100
        if pa.types.is_fixed_size_binary(t):
            return bytes(rng.randrange(256) for _ in range(t.byte_width))
        if pa.types.is_binary(t) or pa.types.is_large_binary(t):
            return bytes(rng.randrange(256) for _ in range(rng.randint(0, 5)))
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            return "".join(rng.choice("xyzé") for _ in range(rng.randint(0, 5)))
        if pa.types.is_temporal(t):
            return rng.randint(0, 20000)
        if pa.types.is_unsigned_integer(t):
            return rng.randint(0, 2 ** min(t.bit_width, 16) - 1)
        return rng.randint(-(2 ** (t.bit_width - 1)), 2 ** (t.bit_width - 1) - 1)

    values = [None if nullable and rng.random() < 0.25 else value() for _ in range(n)]
    if pa.types.is_float16(t):  # half-float bit patterns (no numpy needed)
        return pa.array([None if v is None else rng.randrange(1 << 16) for v in values], pa.uint16()).view(t)
    if pa.types.is_temporal(t):
        storage = pa.int64() if t.bit_width == 64 else pa.int32()
        return pa.array([None if v is None else v * (86_400_000 if pa.types.is_date64(t) else 1) for v in values],
                        storage).cast(t)
    return pa.array(values, t)


def random_array(t: pa.DataType, n: int, nullable: bool, rng: random.Random) -> pa.Array:
    """An array of `n` elements of type `t`, sliced out of a longer one (non-zero offsets)."""
    pad = rng.randint(0, 3)
    total = n + pad
    mask = pa.array([rng.random() < 0.25 for _ in range(total)], pa.bool_()) if nullable else None
    if isinstance(t, pa.BaseExtensionType):
        arr = pa.ExtensionArray.from_storage(t, random_array(t.storage_type, total, nullable, rng))
    elif pa.types.is_dictionary(t):
        values = random_array(t.value_type, rng.randint(1, 4), nullable and rng.random() < 0.3, rng)
        index_values = [None if nullable and rng.random() < 0.2 else rng.randrange(len(values)) for _ in range(total)]
        arr = pa.DictionaryArray.from_arrays(pa.array(index_values, t.index_type), values)
    elif pa.types.is_list(t) or pa.types.is_large_list(t):
        lengths = [rng.choice([0, 0, 1, 2, 3]) for _ in range(total)]
        offsets = [0]
        for length in lengths:
            offsets.append(offsets[-1] + length)
        child = random_array(t.value_type, offsets[-1], t.value_field.nullable, rng)
        cls = pa.LargeListArray if pa.types.is_large_list(t) else pa.ListArray
        index_type = pa.int64() if pa.types.is_large_list(t) else pa.int32()
        arr = cls.from_arrays(pa.array(offsets, index_type), child, type=t, mask=mask)
    elif pa.types.is_fixed_size_list(t):
        child = random_array(t.value_type, total * t.list_size, t.value_field.nullable, rng)
        validity = None if mask is None else pc.invert(mask).buffers()[1]
        arr = pa.Array.from_buffers(t, total, [validity], null_count=-1, children=[child])  # also n = 0
    elif pa.types.is_struct(t) and t.num_fields == 0:
        validity = None if mask is None else pc.invert(mask).buffers()[1]
        arr = pa.Array.from_buffers(t, total, [validity], null_count=-1)
    elif pa.types.is_struct(t):
        children = [random_array(f.type, total, f.nullable, rng) for f in t]
        arr = pa.StructArray.from_arrays(children, fields=list(t), mask=mask)
    else:
        arr = _leaf_values(t, total, nullable, rng)
    return arr.slice(rng.randint(0, pad), n)


def random_table(rng: random.Random, schema: pa.Schema | None = None) -> pa.Table:
    """A table of a random (or the given) schema, split into 1-3 batches (some may be empty)."""
    schema = schema if schema is not None else random_schema(rng)
    rows = rng.choice([0, 1, 1, 2, 5, 17])
    columns = [random_array(f.type, rows, f.nullable, rng) for f in schema]
    batch = pa.RecordBatch.from_arrays(columns, schema=schema) if columns else pa.RecordBatch.from_pylist(
        [{}] * rows, schema=schema
    )
    cuts = sorted(rng.choices(range(rows + 1), k=rng.randint(0, 2)))
    pieces = [batch.slice(a, b - a) for a, b in zip([0, *cuts], [*cuts, rows], strict=True)]
    return pa.Table.from_batches(pieces, schema=schema)
