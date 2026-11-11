"""Performance regression guards for ArrowDigester (ITL-908).

These tests are deterministic: they check memory and operation counts, not wall-clock time.
Hash values are pinned by the golden tests; nothing here may change a hash.
"""

from __future__ import annotations

import tracemalloc

import pyarrow as pa

from starfix.arrow_digester import ArrowDigester


def _list_column(rows: int, per_row: int) -> pa.Table:
    values = pa.array(range(rows * per_row), pa.int64())
    offsets = pa.array(range(0, rows * per_row + 1, per_row), pa.int32())
    col = pa.ListArray.from_arrays(offsets, values)
    return pa.table({"c": col}, schema=pa.schema([pa.field("c", col.type, nullable=False)]))


def test_list_leaf_hashing_never_copies_the_shared_values_buffer() -> None:
    """Hashing a row's slice must not copy the whole child buffer (that made lists quadratic).

    The child values buffer here is 1.6 MB. Copying it, even once per row, allocates at
    least that much; hashing per-row slices of it must stay far below.
    """
    table = _list_column(rows=2_000, per_row=100)
    buffer_size = table.column(0).chunk(0).values.nbytes
    assert buffer_size == 1_600_000

    # Warm up first: the first call lazily imports pyarrow.compute (hundreds of KB of
    # Python objects), which must not count towards the peak.
    ArrowDigester.hash_table(_list_column(rows=2, per_row=2))

    tracemalloc.start()
    try:
        ArrowDigester.hash_table(table)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < buffer_size // 4, f"peak {peak} bytes: the shared values buffer was copied"


def _count_traversals(monkeypatch, table: pa.Table) -> int:
    import starfix.arrow_digester as ad

    calls = 0
    original = ad._traverse_and_update

    def counting(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(ad, "_traverse_and_update", counting)
    ArrowDigester.hash_table(table)
    monkeypatch.setattr(ad, "_traverse_and_update", original)
    return calls


def _nested_table(rows: int) -> pa.Table:
    t = pa.list_(pa.field("item", pa.struct([("a", pa.int16()), ("b", pa.list_(pa.utf8()))])))
    col = pa.array(
        [None if i % 7 == 0 else [{"a": i, "b": ["x"] * (i % 3)}] * (i % 4) for i in range(rows)], t
    )
    return pa.table({"c": col})


def test_list_traversal_work_does_not_grow_with_rows(monkeypatch) -> None:
    """Lists are traversed once per node per batch, not once per row."""
    small = _count_traversals(monkeypatch, _nested_table(10))
    large = _count_traversals(monkeypatch, _nested_table(10_000))
    assert small == large, f"{small} traversals for 10 rows, {large} for 10,000 rows"
