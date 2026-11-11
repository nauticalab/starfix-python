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

    # Respect tracing started by the caller (e.g. -X tracemalloc): measure the peak
    # above the current usage, and stop tracing only if this test started it.
    started_here = not tracemalloc.is_tracing()
    if started_here:
        tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        baseline, _ = tracemalloc.get_traced_memory()
        ArrowDigester.hash_table(table)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        if started_here:
            tracemalloc.stop()
    growth = peak - baseline
    assert growth < buffer_size // 4, f"peak grew by {growth} bytes: the shared values buffer was copied"
