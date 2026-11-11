"""Differential test: starfix.legacy.v0_4 against the released starfix-python 0.4.0.

Usage (from the repository root):

    uv run python scripts/legacy_differential.py [--seeds N]

The driver hashes seeded random inputs with ``starfix.legacy.v0_4`` (this checkout), then runs
this same script with ``--impl release`` in an isolated environment containing the released
``starfix==0.4.0`` from PyPI and the same pyarrow version, and requires every result to be
byte-identical (exceptions must match by type). Both processes build the inputs themselves
from the seeds, so sliced arrays keep their offsets. Exit status 0 means identical.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import subprocess
import sys
import tempfile
from pathlib import Path

import pyarrow as pa

RELEASE_SHA256 = "fc67e0622b484d3dd3ba94bf906571d1bd3565eb854d40feb2da1dcbaa431bec"  # 0.4.0 arrow_digester.py

_LEAVES = [
    pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint64(), pa.float32(), pa.float64(),
    pa.date32(), pa.timestamp("us"), pa.timestamp("ms", tz="UTC"), pa.duration("s"), pa.time32("ms"),
    pa.decimal128(10, 2), pa.binary(4), pa.utf8(), pa.large_utf8(), pa.binary(), pa.bool_(), "dict", "dict_int",
]


def _random_type(rng: random.Random, depth: int) -> pa.DataType:
    kind = rng.random()
    if depth >= 3 or kind < 0.45:
        leaf = rng.choice(_LEAVES)
        if leaf == "dict":
            return pa.dictionary(pa.int32(), pa.utf8())
        if leaf == "dict_int":
            return pa.dictionary(pa.int32(), pa.int64())
        return leaf
    if kind < 0.75:
        element = pa.field("item", _random_type(rng, depth + 1), nullable=rng.random() < 0.6)
        return pa.large_list(element) if rng.random() < 0.3 else pa.list_(element)
    names = rng.sample(["a", "b", "c", "d"], rng.randint(1, 3))
    return pa.struct([pa.field(n, _random_type(rng, depth + 1), nullable=rng.random() < 0.6) for n in names])


def _leaf_values(t: pa.DataType, n: int, nullable: bool, rng: random.Random) -> pa.Array:
    from decimal import Decimal

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
        if pa.types.is_dictionary(t):
            return rng.choice(["p", "q", "r"] if pa.types.is_string(t.value_type) else [7, 8, 9])
        if pa.types.is_temporal(t):
            return rng.randint(0, 20000)
        if pa.types.is_unsigned_integer(t):
            return rng.randint(0, 2 ** min(t.bit_width, 16) - 1)
        return rng.randint(-(2 ** (t.bit_width - 1)), 2 ** (t.bit_width - 1) - 1)

    values = [None if nullable and rng.random() < 0.25 else value() for _ in range(n)]
    if pa.types.is_dictionary(t):
        return pa.array(values, t.value_type).dictionary_encode()
    if pa.types.is_temporal(t) and not pa.types.is_date(t):
        return pa.array(values, pa.int64() if t.bit_width == 64 else pa.int32()).cast(t)
    if pa.types.is_date(t):
        return pa.array(values, pa.int32()).cast(t)
    return pa.array(values, t)


def _random_array(t: pa.DataType, n: int, nullable: bool, rng: random.Random) -> pa.Array:
    pad = rng.randint(0, 3)
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
        arr = cls.from_arrays(pa.array(offsets, index_type), child, mask=mask)
        if arr.type != t:
            arr = arr.cast(t)
    elif pa.types.is_struct(t):
        children = [_random_array(f.type, total, f.nullable, rng) for f in t]
        arr = pa.StructArray.from_arrays(children, fields=list(t), mask=mask)
    else:
        arr = _leaf_values(t, total, nullable, rng)
    return arr.slice(rng.randint(0, pad), n)


def _metadata(rng: random.Random) -> dict[str, str] | None:
    if rng.random() < 0.5:
        return None
    return {rng.choice(["unit", "z", "é", "a"]): rng.choice(["kg", "", "v1", "ü"]) for _ in range(rng.randint(1, 2))}


def _random_batch(rng: random.Random, rows: int) -> pa.RecordBatch:
    fields, columns = [], []
    for i in range(rng.randint(1, 3)):
        t = _random_type(rng, 0)
        nullable = rng.random() < 0.6
        fields.append(pa.field(f"col{i}", t, nullable=nullable, metadata=_metadata(rng)))
        columns.append(_random_array(t, rows, nullable, rng))
    return pa.record_batch(columns, schema=pa.schema(fields, metadata=_metadata(rng)))


def _quirk_cases():
    """Inputs on which starfix-python 0.4.0 differed from Rust 0.4.0 (kept in legacy)."""
    value = pa.struct([pa.field("z", pa.int32()), pa.field("a", pa.int8())])
    map_schema = pa.schema([pa.field("m", pa.map_(pa.utf8(), pa.field("value", value, metadata={"u": "kg"})))])
    yield "map_schema", lambda D: D.hash_schema(map_schema)
    yield "map_schema_metadata", lambda D: D.hash_schema(map_schema, include_metadata=True)
    non_ascii = pa.StructArray.from_arrays([pa.array([1], pa.int32())], fields=[pa.field("é", pa.int32())])
    yield "non_ascii_hash_array", lambda D: D.hash_array(non_ascii)
    for name, t in (("null", pa.null()), ("interval", pa.month_day_nano_interval()), ("utf8_view", pa.string_view())):
        yield f"schema_{name}", lambda D, t=t: D.hash_schema(pa.schema([pa.field("c", t)]))
    child = pa.array([10, 20, 30, 10]).dictionary_encode()
    lst = pa.ListArray.from_arrays(pa.array([0, 2, 3, 4], pa.int32()), child)
    yield "list_of_int_dictionary", lambda D: D.hash_array(lst)
    good = pa.record_batch([pa.array([1], pa.int32())], names=["a"])
    other = pa.record_batch([pa.array([1], pa.int64())], names=["a"])

    def unchecked_stream(D):
        d = D(good.schema)
        d.update(other)  # 0.4.0 does not check batches against the schema
        return d.finalize()

    yield "stream_without_batch_check", unchecked_stream
    # A zero-length fixed-width array with no values buffer: 0.4.0 raises AttributeError.
    yield "fixed_width_without_values_buffer", lambda D: D.hash_array(pa.Array.from_buffers(pa.int32(), 0, [None, None]))


def _cases(seeds: int):
    yield from _quirk_cases()
    for seed in range(seeds):
        rng = random.Random(seed)
        rows = rng.choice([0, 1, 2, 5, 17, 40])
        batch = _random_batch(rng, rows)
        cuts = sorted(rng.sample(range(rows + 1), k=min(rows + 1, rng.randint(1, 3))))
        pieces = [batch.slice(a, b - a) for a, b in zip([0, *cuts], [*cuts, rows], strict=True)]

        def stream(D, schema=batch.schema, pieces=pieces):
            d = D(schema)
            for piece in pieces:
                d.update(piece)
            return d.finalize()

        yield f"{seed}:batch", lambda D, b=batch: D.hash_record_batch(b)
        yield f"{seed}:batch_metadata", lambda D, b=batch: D.hash_record_batch(b, include_metadata=True)
        yield f"{seed}:schema_metadata", lambda D, b=batch: D.hash_schema(b.schema, include_metadata=True)
        yield f"{seed}:stream", stream
        yield f"{seed}:table", lambda D, s=batch.schema, p=pieces: D.hash_table(pa.Table.from_batches(p, schema=s))
        for i, column in enumerate(batch.columns):
            yield f"{seed}:array{i}", lambda D, c=column: D.hash_array(c)


def _run(impl: str, seeds: int) -> dict[str, str]:
    if impl == "release":
        import starfix.arrow_digester as module

        digest = hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        if digest != RELEASE_SHA256:
            raise SystemExit(f"not the starfix 0.4.0 release module: {module.__file__} ({digest})")
    else:
        import starfix.legacy.v0_4 as module
    D = module.ArrowDigester
    results = {}
    for name, call in _cases(seeds):
        try:
            results[name] = call(D).hex()
        except Exception as e:  # noqa: BLE001 - exceptions must match by type
            results[name] = f"raises {type(e).__name__}"
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--impl", choices=["driver", "legacy", "release"], default="driver")
    parser.add_argument("--seeds", type=int, default=400)
    parser.add_argument("--out")
    args = parser.parse_args()
    if args.impl != "driver":
        results = _run(args.impl, args.seeds)
        Path(args.out).write_text(json.dumps(results))
        return 0

    legacy = _run("legacy", args.seeds)
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "release.json"
        python = f"{sys.version_info.major}.{sys.version_info.minor}"
        subprocess.run(
            ["uv", "run", "--no-project", "--python", python, "--with", "starfix==0.4.0",
             "--with", f"pyarrow=={pa.__version__}", "python", str(Path(__file__).resolve()),
             "--impl", "release", "--seeds", str(args.seeds), "--out", str(out)],
            check=True, cwd=tmp,
        )
        release = json.loads(out.read_text())
    differ = sorted(k for k in legacy if legacy[k] != release.get(k)) + sorted(set(release) - set(legacy))
    raised = sum(1 for v in legacy.values() if v.startswith("raises"))
    print(f"{len(legacy)} results compared ({raised} raise the same exception in both); {len(differ)} differ")
    for key in differ[:20]:
        print(f"  DIFFER {key}: legacy={legacy.get(key)} release={release.get(key)}")
    return 1 if differ else 0


if __name__ == "__main__":
    sys.exit(main())
