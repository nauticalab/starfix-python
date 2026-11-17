"""Time per hash of small tables: one-shot calls against a prepared schema (ITL-1177).

Usage (from the repository root):

    uv run python scripts/bench_prepared.py [--rows 1] [--repeat 7]

Hashes tables of a few-column schema (ints, strings, a list, a struct, field and schema
metadata), each built anew as a caller hashing row by row would (so every table carries a new
``pa.Schema`` object). Prints the best-of-``--repeat`` mean time per call in microseconds.
Against a checkout without ``ArrowDigester.prepare`` (0.5.0 and earlier) only the one-shot rows
are printed, so the same script measures before and after: for example

    git worktree add /tmp/starfix-main origin/main
    PYTHONPATH=/tmp/starfix-main/src uv run python scripts/bench_prepared.py
"""

from __future__ import annotations

import argparse
import platform
import timeit

import pyarrow as pa

import starfix
from starfix import ArrowDigester

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.large_string()),
        pa.field("score", pa.float64(), metadata={"unit": "points"}),
        pa.field("tags", pa.list_(pa.large_string())),
        pa.field("pos", pa.struct([pa.field("x", pa.float32()), pa.field("y", pa.float32())])),
        pa.field("path", pa.large_string(), metadata={"semantic": "path"}),
    ],
    metadata={"producer": "bench"},
)


def _rows(n: int) -> list[dict]:
    return [
        {"id": i, "name": f"n{i}", "score": i / 3, "tags": ["a", "b"][: i % 3], "pos": {"x": 1.0, "y": 2.0},
         "path": f"/data/{i}.bin"}
        for i in range(n)
    ]


def _bench_options(bench, tables, batches, options: dict[str, bool], has_prepare: bool) -> None:
    bench("one-shot ArrowDigester.hash_table", lambda i: ArrowDigester.hash_table(tables[i], **options))
    bench("one-shot ArrowDigester.hash_record_batch", lambda i: ArrowDigester.hash_record_batch(batches[i], **options))
    if not has_prepare:
        return
    prepared = ArrowDigester.prepare(SCHEMA, **options)
    bench("prepared.hash_table", lambda i: prepared.hash_table(tables[i]))
    bench("prepared.hash_record_batch", lambda i: prepared.hash_record_batch(batches[i]))
    bench("  of which: ArrowDigester.prepare (once per schema)", lambda i: ArrowDigester.prepare(SCHEMA, **options))
    analysis = prepared._analysis
    bench("  of which: same-schema check (identical schema)", lambda i: analysis.matches(tables[i].schema))
    string_schema = pa.schema([f.with_type(pa.string()) if f.type == pa.large_string() else f for f in SCHEMA],
                              metadata=SCHEMA.metadata)
    bench("  same-schema check, hash-equivalent (string) schema", lambda i: analysis.matches(string_schema))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rows", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--number", type=int, default=2000)
    args = parser.parse_args()
    rows = _rows(args.rows)
    tables = [pa.Table.from_pylist(rows, schema=SCHEMA) for _ in range(64)]  # distinct Schema objects
    batches = [t.to_batches()[0] for t in tables]
    has_prepare = hasattr(ArrowDigester, "prepare")
    print(f"starfix from {starfix.__file__}; pyarrow {pa.__version__}; Python {platform.python_version()}; "
          f"{platform.machine()}; {args.rows} row(s), {len(SCHEMA)} columns")

    def bench(label: str, call) -> None:
        it = iter(range(1 << 62))

        def step() -> None:
            call(next(it) % len(tables))

        best = min(timeit.repeat(step, number=args.number, repeat=args.repeat)) / args.number
        print(f"  {label:<52} {best * 1e6:8.1f} us")

    for include_metadata in (False, True):
        print(f"include_metadata={include_metadata}")
        _bench_options(bench, tables, batches, {"include_metadata": include_metadata}, has_prepare)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
