"""Differential test: this checkout (one-shot and prepared) against the released starfix 0.5.0.

Usage (from the repository root):

    uv run python scripts/release_differential.py [--seeds N]

The driver hashes seeded random tables (``tests/random_arrow.py``: nested types, dictionaries,
fixed-size lists, extension types, metadata at every depth, empty and multi-batch tables) under
all 8 option combinations, through the one-shot calls and through ``ArrowDigester.prepare``
(``hash_table``, ``hash_record_batch``, ``digester()`` and ``schema_digest``), and requires the
prepared results to equal the one-shot ones. It then runs this same script with
``--impl release`` in an isolated environment containing the released ``starfix==0.5.0`` from
PyPI and the same pyarrow version, and requires every one-shot result to be byte-identical
(exceptions must match by type). Both processes build the inputs themselves from the seeds.
Exit status 0 means identical.
"""

from __future__ import annotations

import argparse
import itertools
import json
import random
import subprocess
import sys
import tempfile
from importlib import metadata
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
import random_arrow

RELEASE = "0.5.0"
OPTION_SETS = [
    {"include_metadata": m, "ordered_columns": c, "ordered_struct_fields": s}
    for m, c, s in itertools.product([False, True], repeat=3)
]


def _outcome(call, *args, **kwargs) -> str:
    try:
        return call(*args, **kwargs).hex()
    except Exception as e:  # noqa: BLE001 - exceptions must match by type
        return f"raises {type(e).__name__}"


def _stream(digester, batches) -> bytes:
    for batch in batches:
        digester.update(batch)
    return digester.finalize()


def _one_shot_stream(D, schema, batches, **options) -> bytes:
    return _stream(D(schema, **options), batches)


def _prepared_schema(D, schema, **options) -> bytes:
    return D.prepare(schema, **options).schema_digest


def _prepared_table(D, table, **options) -> bytes:
    return D.prepare(table.schema, **options).hash_table(table)


def _prepared_batch(D, schema, batch, **options) -> bytes:
    return D.prepare(schema, **options).hash_record_batch(batch)


def _prepared_stream(D, schema, batches, **options) -> bytes:
    return _stream(D.prepare(schema, **options).digester(), batches)


def _special_tables():
    """Inputs that raise: schemas whose data does not hash, rejected schemas, nulls in
    non-nullable fields."""
    yield "map", pa.table({"m": pa.array([[("k", 1)]], pa.map_(pa.string(), pa.int8()))})
    yield "null", pa.table({"n": pa.nulls(2)})
    yield "union", pa.table({"u": pa.UnionArray.from_sparse(pa.array([0], pa.int8()), [pa.array([1])])})
    yield "duplicate", pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["a", "a"])
    nulls = pa.StructArray.from_arrays([pa.array([1, None])], fields=[pa.field("x", pa.int64(), nullable=False)])
    yield "non_nullable_null", pa.Table.from_arrays([nulls], names=["s"])
    meta = pa.schema([pa.field("a", pa.int8(), metadata={"ARROW:extension:name": "x"})])
    yield "extension_name_without_type", pa.table({"a": pa.array([1], pa.int8())}, schema=meta)


def _tables(seeds: int):
    yield from _special_tables()
    for seed in range(seeds):
        yield seed, random_arrow.random_table(random.Random(seed))


def _run(impl: str, seeds: int) -> tuple[dict[str, str], list[str]]:
    from starfix import ArrowDigester as D

    if impl == "release" and metadata.version("starfix") != RELEASE:
        raise SystemExit(f"not the starfix {RELEASE} release: {metadata.version('starfix')}")
    results: dict[str, str] = {}
    prepared_differ: list[str] = []
    for seed, table in _tables(seeds):
        schema, batches = table.schema, table.to_batches()
        for options in OPTION_SETS:
            tag = f"{seed}:m{int(options['include_metadata'])}c{int(options['ordered_columns'])}" \
                  f"s{int(options['ordered_struct_fields'])}"
            one_shot = {
                "schema": _outcome(D.hash_schema, schema, **options),
                "table": _outcome(D.hash_table, table, **options),
                "stream": _outcome(_one_shot_stream, D, schema, batches, **options),
            }
            if batches:
                one_shot["batch"] = _outcome(D.hash_record_batch, batches[0], **options)
            for name, value in one_shot.items():
                results[f"{tag}:{name}"] = value
            if impl != "current":
                continue
            prepared = {
                "schema": _outcome(_prepared_schema, D, schema, **options),
                "table": _outcome(_prepared_table, D, table, **options),
                "stream": _outcome(_prepared_stream, D, schema, batches, **options),
            }
            if batches:
                prepared["batch"] = _outcome(_prepared_batch, D, schema, batches[0], **options)
            prepared_differ += [f"{tag}:{k}" for k in one_shot if prepared[k] != one_shot[k]]
    return results, prepared_differ


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--impl", choices=["driver", "current", "release"], default="driver")
    parser.add_argument("--seeds", type=int, default=1000)
    parser.add_argument("--out")
    args = parser.parse_args()
    if args.impl != "driver":
        results, _ = _run(args.impl, args.seeds)
        Path(args.out).write_text(json.dumps(results))
        return 0

    current, prepared_differ = _run("current", args.seeds)
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "release.json"
        python = f"{sys.version_info.major}.{sys.version_info.minor}"
        subprocess.run(
            ["uv", "run", "--no-project", "--python", python, "--with", f"starfix=={RELEASE}",
             "--with", f"pyarrow=={pa.__version__}", "python", str(Path(__file__).resolve()),
             "--impl", "release", "--seeds", str(args.seeds), "--out", str(out)],
            check=True, cwd=tmp,
        )
        release = json.loads(out.read_text())
    differ = sorted(k for k in current if current[k] != release.get(k)) + sorted(set(release) - set(current))
    raised = sum(1 for v in current.values() if v.startswith("raises"))
    print(f"{len(current)} one-shot results compared with starfix {RELEASE} ({raised} raise the same exception "
          f"in both); {len(differ)} differ; {len(prepared_differ)} prepared results differ from one-shot")
    for key in differ[:20]:
        print(f"  DIFFER {key}: current={current.get(key)} release={release.get(key)}")
    for key in prepared_differ[:20]:
        print(f"  PREPARED DIFFERS {key}")
    return 1 if differ or prepared_differ else 0


if __name__ == "__main__":
    sys.exit(main())
