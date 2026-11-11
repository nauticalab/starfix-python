"""Hash-neutrality of the vectorised digester (ITL-908).

Every input is hashed by ``starfix.arrow_digester`` and by the frozen per-row reference
in ``tests/_reference_digester.py`` (a verbatim copy of the implementation whose hashes
the golden tests pin). The two must agree byte for byte.

Inputs are random but seeded (reproducible). They cover nested lists, large lists,
structs, dictionaries, Booleans and every leaf width, with nulls at every level, null
list slots whose offsets range is non-empty, empty lists, zero rows, arrays sliced to
non-zero offsets, multi-batch streaming and chunked tables.
"""

from __future__ import annotations

import random

import pyarrow as pa
import pytest
from _reference_digester import ArrowDigester as ReferenceDigester

from starfix.arrow_digester import ArrowDigester

_LEAVES = [
    pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint64(),
    pa.float32(), pa.float64(), pa.date32(), pa.timestamp("us"), pa.decimal128(10, 2),
    pa.binary(4), pa.utf8(), pa.large_utf8(), pa.binary(), pa.bool_(), "dict",
]


def _random_type(rng: random.Random, depth: int) -> pa.DataType:
    kind = rng.random()
    if depth >= 3 or kind < 0.45:
        leaf = rng.choice(_LEAVES)
        return pa.dictionary(pa.int32(), pa.utf8()) if leaf == "dict" else leaf
    if kind < 0.75:
        element = pa.field("item", _random_type(rng, depth + 1), nullable=rng.random() < 0.6)
        return pa.large_list(element) if rng.random() < 0.3 else pa.list_(element)
    names = rng.sample(["a", "b", "c", "d"], rng.randint(1, 3))
    return pa.struct([
        pa.field(n, _random_type(rng, depth + 1), nullable=rng.random() < 0.6) for n in names
    ])


def _leaf_values(t: pa.DataType, n: int, nullable: bool, rng: random.Random) -> pa.Array:
    def value(i):
        if pa.types.is_boolean(t):
            return rng.random() < 0.5
        if pa.types.is_floating(t):
            return rng.uniform(-1e3, 1e3)
        if pa.types.is_decimal(t):
            from decimal import Decimal
            return Decimal(rng.randint(-99999, 99999)) / 100
        if pa.types.is_fixed_size_binary(t):
            return bytes(rng.randrange(256) for _ in range(t.byte_width))
        if pa.types.is_binary(t) or pa.types.is_large_binary(t):
            return bytes(rng.randrange(256) for _ in range(rng.randint(0, 5)))
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            return "".join(rng.choice("xyzé") for _ in range(rng.randint(0, 5)))
        if pa.types.is_dictionary(t):
            return rng.choice(["p", "q", "r"])
        if pa.types.is_timestamp(t) or pa.types.is_date(t):
            return rng.randint(0, 20000)
        if pa.types.is_unsigned_integer(t):
            return rng.randint(0, 2 ** min(t.bit_width, 16) - 1)
        return rng.randint(-(2 ** (t.bit_width - 1)), 2 ** (t.bit_width - 1) - 1)

    values = [None if nullable and rng.random() < 0.25 else value(i) for i in range(n)]
    if pa.types.is_dictionary(t):
        return pa.array(values, pa.utf8()).dictionary_encode()
    if pa.types.is_timestamp(t) or pa.types.is_date(t):
        return pa.array(values, pa.int64() if pa.types.is_timestamp(t) else pa.int32()).cast(t)
    return pa.array(values, t)


def _random_array(t: pa.DataType, n: int, nullable: bool, rng: random.Random) -> pa.Array:
    """An array of `t` with `n` rows; nested arrays are built bottom-up and sliced."""
    pad = rng.randint(0, 3)  # build extra rows and slice them off: non-zero offsets
    total = n + pad
    mask = pa.array([rng.random() < 0.25 for _ in range(total)], pa.bool_()) if nullable else None
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        lengths = [rng.choice([0, 0, 1, 2, 3]) for _ in range(total)]
        offsets = [0]
        for length in lengths:
            offsets.append(offsets[-1] + length)
        child = _random_array(t.value_type, offsets[-1], t.value_field.nullable, rng)
        cls = pa.LargeListArray if pa.types.is_large_list(t) else pa.ListArray
        index_type = pa.int64() if pa.types.is_large_list(t) else pa.int32()
        # Null slots keep their (possibly non-empty) offsets range.
        arr = cls.from_arrays(pa.array(offsets, index_type), child, mask=mask)
        if arr.type != t:  # keep the element field's name and nullability
            arr = arr.cast(t)
    elif pa.types.is_struct(t):
        children = [_random_array(f.type, total, f.nullable, rng) for f in t]
        arr = pa.StructArray.from_arrays(children, fields=list(t), mask=mask)
    else:
        arr = _leaf_values(t, total, nullable, rng)
    start = rng.randint(0, pad)
    return arr.slice(start, n)


def _random_batch(rng: random.Random, rows: int) -> pa.RecordBatch:
    fields, columns = [], []
    for i in range(rng.randint(1, 3)):
        t = _random_type(rng, 0)
        nullable = rng.random() < 0.6
        fields.append(pa.field(f"col{i}", t, nullable=nullable))
        columns.append(_random_array(t, rows, nullable, rng))
    return pa.record_batch(columns, schema=pa.schema(fields))


_SEEDS = range(400)


@pytest.mark.parametrize("seed", _SEEDS)
def test_vectorised_matches_reference(seed: int) -> None:
    rng = random.Random(seed)
    rows = rng.choice([0, 1, 2, 5, 17, 40])
    batch = _random_batch(rng, rows)

    assert ArrowDigester.hash_record_batch(batch) == ReferenceDigester.hash_record_batch(batch)

    # Multi-batch streaming and chunked tables: random split points.
    cuts = sorted(rng.sample(range(rows + 1), k=min(rows + 1, rng.randint(1, 3))))
    pieces = [batch.slice(a, b - a) for a, b in zip([0, *cuts], [*cuts, rows])]
    new, ref = ArrowDigester(batch.schema), ReferenceDigester(batch.schema)
    for piece in pieces:
        new.update(piece)
        ref.update(piece)
    assert new.finalize() == ref.finalize()
    table = pa.Table.from_batches(pieces, schema=batch.schema)
    assert ArrowDigester.hash_table(table) == ReferenceDigester.hash_table(table)

    for column in batch.columns:
        assert ArrowDigester.hash_array(column) == ReferenceDigester.hash_array(column)


def test_generator_covers_the_required_shapes() -> None:
    """The seeds above really contain the shapes the equivalence must hold for."""
    seen = set()
    for seed in _SEEDS:
        rng = random.Random(seed)
        rows = rng.choice([0, 1, 2, 5, 17, 40])
        batch = _random_batch(rng, rows)
        if rows == 0:
            seen.add("zero_rows")
        for column in batch.columns:
            if column.offset:
                seen.add("non_zero_offset")
            stack = [column]
            while stack:
                a = stack.pop()
                if isinstance(a, (pa.ListArray, pa.LargeListArray)):
                    seen.add("list")
                    offs = a.offsets.to_pylist()
                    for i in range(len(a)):
                        if a[i].is_valid and offs[i + 1] == offs[i]:
                            seen.add("empty_list")
                        if not a[i].is_valid and offs[i + 1] > offs[i]:
                            seen.add("null_slot_with_range")
                    if pa.types.is_boolean(a.type.value_type):
                        seen.add("boolean_under_list")
                    stack.append(a.values)
                elif isinstance(a, pa.StructArray):
                    seen.add("struct")
                    if a.null_count:
                        seen.add("null_struct")
                    stack.extend(a.field(i) for i in range(a.type.num_fields))
    required = {
        "zero_rows", "non_zero_offset", "list", "empty_list", "null_slot_with_range",
        "boolean_under_list", "struct", "null_struct",
    }
    assert required <= seen, f"missing shapes: {required - seen}"
