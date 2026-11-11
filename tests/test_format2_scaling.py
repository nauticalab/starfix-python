"""Deterministic scaling guards for hash format 2 (ITL-908 regression, ported to format 2).

Format 1 hashed list columns in quadratic time (each row copied the whole child values
buffer). These tests check memory and operation counts, not wall-clock time.
"""

from __future__ import annotations

import tracemalloc

import pyarrow as pa

from starfix import _format2 as f2


def _list_table(rows: int, per_row: int) -> pa.Table:
    values = pa.array(range(rows * per_row), pa.int64())
    offsets = pa.array(range(0, rows * per_row + 1, per_row), pa.int32())
    col = pa.ListArray.from_arrays(offsets, values)
    return pa.table({"c": col}, schema=pa.schema([pa.field("c", col.type, nullable=False)]))


def test_list_hashing_never_copies_the_shared_values_buffer() -> None:
    table = _list_table(rows=2_000, per_row=100)
    buffer_size = table.column(0).chunk(0).values.nbytes
    assert buffer_size == 1_600_000
    f2.hash_table(_list_table(rows=2, per_row=2))  # warm up lazy imports

    started_here = not tracemalloc.is_tracing()
    if started_here:
        tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        baseline, _ = tracemalloc.get_traced_memory()
        f2.hash_table(table)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        if started_here:
            tracemalloc.stop()
    growth = peak - baseline
    assert growth < buffer_size // 4, f"peak grew by {growth} bytes: the values buffer was copied"


def _nested_table(rows: int) -> pa.Table:
    t = pa.list_(pa.field("item", pa.struct([("a", pa.int16()), ("b", pa.list_(pa.utf8())), ("c", pa.bool_())])))
    col = pa.array(
        [None if i % 7 == 0 else [{"a": i, "b": ["x"] * (i % 3), "c": i % 2 == 0}] * (i % 4) for i in range(rows)], t
    )
    return pa.table({"c": col})


def test_traversal_count_does_not_grow_with_rows(monkeypatch) -> None:
    calls = 0
    original = f2._visit

    def counting(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(f2, "_visit", counting)
    counts = []
    for rows in (10, 10_000):
        calls = 0
        f2.hash_table(_nested_table(rows))
        counts.append(calls)
    assert counts[0] == counts[1], f"{counts[0]} visits for 10 rows, {counts[1]} for 10,000 rows"


def test_nullable_list_of_binary_does_not_copy_the_payload() -> None:
    """A list column with a null row is not filtered as a whole (no second payload copy).

    Arrow allocations are invisible to tracemalloc, so they are measured on a fresh proxy of
    the Arrow memory pool, made the default for the duration of the hash.
    """
    values = pa.array([b"x" * 1000] * 4000, pa.binary())  # 4 MB payload
    offsets = pa.array(range(0, 4001, 4), pa.int32())
    mask = pa.array([i == 0 for i in range(1000)])
    table = pa.table({"c": pa.ListArray.from_arrays(offsets, values, mask=mask)})
    payload = values.buffers()[2].size
    f2.hash_table(table.slice(0, 2))  # warm up

    default = pa.default_memory_pool()
    pool = pa.proxy_memory_pool(default)
    pa.set_memory_pool(pool)
    try:
        f2.hash_table(table)
    finally:
        pa.set_memory_pool(default)
    assert pool.max_memory() < payload // 4, f"Arrow peak {pool.max_memory()} bytes for a {payload}-byte payload"


def test_dictionary_leaves_are_not_decoded_as_a_whole() -> None:
    """Few large dictionary values referenced many times: hashed from the dictionary buffers."""
    big = pa.array([b"x" * (1 << 20), b"y" * 10, None], pa.large_binary())  # one 1 MiB value
    indices = pa.array([0, 1, 0, None, 2] * 40, pa.int32())  # 80 references to the 1 MiB value
    arr = pa.DictionaryArray.from_arrays(indices, big)
    decoded_size = 80 * (1 << 20)
    f2.hash_array(arr.slice(0, 2))  # warm up

    default = pa.default_memory_pool()
    pool = pa.proxy_memory_pool(default)
    pa.set_memory_pool(pool)
    try:
        f2.hash_array(arr)
        f2.hash_record_batch(pa.record_batch([arr], names=["d"]))
    finally:
        pa.set_memory_pool(default)
    assert pool.max_memory() < decoded_size // 20, f"Arrow peak {pool.max_memory()} bytes (decoded: {decoded_size})"
