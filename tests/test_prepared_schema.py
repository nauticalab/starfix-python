"""A prepared schema: analyse once, hash many inputs of that schema (ITL-1177).

Every digest of the prepared path must be byte-identical to the one-shot path for the same input
and options; a batch or table whose schema differs from the prepared one under the prepared
options is refused; starfix holds no state of its own between calls.
"""

from __future__ import annotations

import base64
import copy
import io
import itertools
import json
import pickle
import random
from functools import cache
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
import random_arrow as ra  # tests/ is on sys.path (pytest rootdir-relative import)

import starfix
from starfix import ArrowDigester, PreparedSchema, _format2, arrow_digester

OPTION_SETS = [
    {"include_metadata": m, "ordered_columns": c, "ordered_struct_fields": s}
    for m, c, s in itertools.product([False, True], repeat=3)
]
_OPTION_IDS = [f"m{int(o['include_metadata'])}c{int(o['ordered_columns'])}s{int(o['ordered_struct_fields'])}"
               for o in OPTION_SETS]
_BATCH_MISMATCH = "^Record batch schema does not match the prepared schema$"
_TABLE_MISMATCH = "^Table schema does not match the prepared schema$"
_STREAM_MISMATCH = "^Record batch schema does not match ArrowDigester schema$"


def _one_shot_stream(schema: pa.Schema, batches: list[pa.RecordBatch], **options: bool) -> bytes:
    d = ArrowDigester(schema, **options)
    for batch in batches:
        d.update(batch)
    return d.finalize()


def _prepared_stream(prepared: PreparedSchema, batches: list[pa.RecordBatch]) -> bytes:
    d = prepared.digester()
    for batch in batches:
        d.update(batch)
    return d.finalize()


def _assert_same_as_one_shot(table: pa.Table, options: dict[str, bool], prepared: PreparedSchema | None = None) -> None:
    """Every prepared entry point equals its one-shot counterpart on `table` (and its batches)."""
    prepared = prepared if prepared is not None else ArrowDigester.prepare(table.schema, **options)
    batches = table.to_batches()
    expected = ArrowDigester.hash_table(table, **options)
    assert prepared.hash_table(table) == expected
    assert _prepared_stream(prepared, batches) == expected
    assert _one_shot_stream(table.schema, batches, **options) == expected
    for batch in batches:
        assert prepared.hash_record_batch(batch) == ArrowDigester.hash_record_batch(batch, **options)


def _simple_table() -> pa.Table:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False, metadata={"role": "key"}),
            pa.field("name", pa.string()),
            pa.field("tags", pa.list_(pa.field("item", pa.string(), metadata={"k": "v"}))),
            pa.field("s", pa.struct([pa.field("b", pa.float64()), pa.field("a", pa.int8())])),
        ],
        metadata={"origin": "test"},
    )
    return pa.table(
        {"id": [1, 2, 3], "name": ["x", None, "z"], "tags": [["p"], [], None], "s": [{"b": 1.0, "a": 1}, None, {"b": None, "a": 3}]},
        schema=schema,
    )


# ---------------------------------------------------------------------------
# The interface
# ---------------------------------------------------------------------------

def test_prepared_schema_is_exported() -> None:
    assert starfix.PreparedSchema is PreparedSchema
    assert "PreparedSchema" in starfix.__all__


@pytest.mark.parametrize("options", OPTION_SETS, ids=_OPTION_IDS)
def test_prepare_echoes_schema_and_options(options: dict[str, bool]) -> None:
    table = _simple_table()
    prepared = ArrowDigester.prepare(table.schema, **options)
    assert isinstance(prepared, PreparedSchema)
    assert prepared.schema.equals(table.schema, check_metadata=True)
    for name, value in options.items():
        assert getattr(prepared, name) is value
    assert prepared.schema_digest == ArrowDigester.hash_schema(table.schema, **options)
    assert len(prepared.schema_digest) == 35


@pytest.mark.parametrize("options", OPTION_SETS, ids=_OPTION_IDS)
def test_prepared_hashes_equal_one_shot(options: dict[str, bool]) -> None:
    table = _simple_table()
    _assert_same_as_one_shot(table, options)
    _assert_same_as_one_shot(table.slice(1, 1), options)
    _assert_same_as_one_shot(table.slice(0, 0), options)
    two_batches = pa.Table.from_batches([table.slice(0, 1).to_batches()[0], table.slice(1).to_batches()[0]])
    _assert_same_as_one_shot(two_batches, options)


def test_prepare_options_are_keyword_only() -> None:
    with pytest.raises(TypeError):
        ArrowDigester.prepare(_simple_table().schema, True)  # type: ignore[misc]


def test_prepared_schema_cannot_be_constructed_directly() -> None:
    with pytest.raises(TypeError, match=r"ArrowDigester\.prepare"):
        PreparedSchema()  # type: ignore[call-arg]


def test_prepared_schema_is_immutable() -> None:
    prepared = ArrowDigester.prepare(_simple_table().schema)
    for name in ("schema", "schema_digest", "include_metadata", "_analysis", "anything"):
        with pytest.raises(AttributeError):
            setattr(prepared, name, None)
    with pytest.raises(AttributeError):
        del prepared.schema
    assert not hasattr(prepared, "__dict__")


def test_prepared_schema_copies_and_pickles() -> None:
    table = _simple_table()
    options = {"include_metadata": True, "ordered_columns": True, "ordered_struct_fields": False}
    prepared = ArrowDigester.prepare(table.schema, **options)
    assert copy.copy(prepared) is prepared
    assert copy.deepcopy(prepared) is prepared
    restored = pickle.loads(pickle.dumps(prepared))  # re-prepared from (schema, options)
    assert restored is not prepared
    assert restored.schema_digest == prepared.schema_digest
    assert restored.hash_table(table) == prepared.hash_table(table)
    for name, value in options.items():
        assert getattr(restored, name) is value


def test_digester_returns_a_fresh_arrow_digester() -> None:
    table = _simple_table()
    prepared = ArrowDigester.prepare(table.schema)
    first, second = prepared.digester(), prepared.digester()
    assert isinstance(first, ArrowDigester) and first is not second
    first.update(table.to_batches()[0])
    assert second.finalize() == ArrowDigester.hash_table(table.slice(0, 0))
    assert first.finalize() == ArrowDigester.hash_table(table)


def test_wrong_container_types() -> None:
    table = _simple_table()
    prepared = ArrowDigester.prepare(table.schema)
    with pytest.raises(TypeError, match="use hash_table for a Table"):
        prepared.hash_record_batch(table)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="use hash_record_batch for a RecordBatch"):
        prepared.hash_table(table.to_batches()[0])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="a pyarrow.Schema"):
        ArrowDigester.prepare(table)  # type: ignore[arg-type]


def test_prepare_raises_what_hash_schema_raises() -> None:
    duplicate = pa.schema([pa.field("a", pa.int8()), pa.field("a", pa.int8())])
    with pytest.raises(ValueError, match="Ambiguous node path"):
        ArrowDigester.hash_schema(duplicate)
    with pytest.raises(ValueError, match="Ambiguous node path"):
        ArrowDigester.prepare(duplicate)
    rejected = pa.schema([pa.field("u", pa.dense_union([pa.field("x", pa.int8())]))])
    with pytest.raises(ValueError, match="Unsupported Arrow type"):
        ArrowDigester.prepare(rejected)


@pytest.mark.parametrize(
    "data_type",
    [pa.null(), pa.map_(pa.string(), pa.int8()), pa.month_day_nano_interval(), pa.string_view(), pa.list_(pa.null())],
    ids=str,
)
def test_schema_only_types_prepare_but_do_not_hash_data(data_type: pa.DataType) -> None:
    schema = pa.schema([pa.field("c", data_type)])
    prepared = ArrowDigester.prepare(schema)
    assert prepared.schema_digest == ArrowDigester.hash_schema(schema)
    table = pa.table({"c": pa.array([], data_type)})
    match = r"^Hashing data of type .* \(node 'c/?'\) is not supported$"
    for call in (lambda: prepared.digester(), lambda: prepared.hash_table(table),
                 lambda: prepared.hash_record_batch(pa.RecordBatch.from_pylist([], schema=table.schema))):
        with pytest.raises(ValueError, match=match):
            call()
    with pytest.raises(ValueError, match=match):
        ArrowDigester(schema)  # unchanged: the streaming constructor still raises


# ---------------------------------------------------------------------------
# No state outside the prepared object
# ---------------------------------------------------------------------------

def _module_state() -> dict[str, dict[str, int]]:
    return {m.__name__: {k: id(v) for k, v in vars(m).items()} for m in (_format2, arrow_digester, starfix)}


def test_no_module_level_state_is_created_or_changed() -> None:
    before = _module_state()
    table = _simple_table()
    for options in OPTION_SETS:
        prepared = ArrowDigester.prepare(table.schema, **options)
        prepared.hash_table(table)
        ArrowDigester.hash_table(table, **options)
        prepared.digester().update(table.to_batches()[0])
    assert _module_state() == before


def test_preparing_a_second_schema_does_not_affect_the_first() -> None:
    table = _simple_table()
    first = ArrowDigester.prepare(table.schema)
    expected = first.hash_table(table)
    other = pa.table({"q": [1.5]})
    ArrowDigester.prepare(other.schema).hash_table(other)
    assert first.hash_table(table) == expected


# ---------------------------------------------------------------------------
# The analysis runs once
# ---------------------------------------------------------------------------

@pytest.fixture
def analysis_calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    counts = {"schema_nodes": 0, "phase1_json": 0, "phase2_json": 0}
    for name in counts:
        original = getattr(_format2, name)

        def counting(*args: Any, _name: str = name, _original: Any = original, **kwargs: Any) -> Any:
            counts[_name] += 1
            return _original(*args, **kwargs)

        monkeypatch.setattr(_format2, name, counting)
    return counts


def test_one_shot_calls_analyse_once(analysis_calls: dict[str, int]) -> None:
    table = _simple_table()
    batch = table.to_batches()[0]
    for call in (lambda: ArrowDigester.hash_table(table, include_metadata=True),
                 lambda: ArrowDigester.hash_record_batch(batch, include_metadata=True),
                 lambda: ArrowDigester.hash_schema(table.schema, include_metadata=True)):
        for name in analysis_calls:
            analysis_calls[name] = 0
        call()
        assert analysis_calls == {"schema_nodes": 1, "phase1_json": 1, "phase2_json": 1}


def test_prepared_hashes_of_an_identical_schema_do_not_analyse(analysis_calls: dict[str, int]) -> None:
    table = _simple_table()
    prepared = ArrowDigester.prepare(table.schema, include_metadata=True)
    rebuilt = pa.table(table.to_pydict(), schema=_simple_table().schema)  # equal schema, other objects
    for name in analysis_calls:
        analysis_calls[name] = 0
    prepared.hash_table(table)
    prepared.hash_table(rebuilt)
    prepared.hash_record_batch(rebuilt.to_batches()[0])
    d = prepared.digester()
    d.update(rebuilt.to_batches()[0])
    d.finalize()
    streaming = ArrowDigester(table.schema, include_metadata=True)
    for name in analysis_calls:
        analysis_calls[name] = 0
    streaming.update(rebuilt.to_batches()[0])
    assert analysis_calls == {"schema_nodes": 0, "phase1_json": 0, "phase2_json": 0}


# ---------------------------------------------------------------------------
# The same-schema rule
# ---------------------------------------------------------------------------

def _field(name: str, t: pa.DataType, nullable: bool = True, metadata: dict[str, str] | None = None) -> pa.Field:
    return pa.field(name, t, nullable=nullable, metadata=metadata)


def _tensor(dim_names: list[str] | None) -> pa.DataType:
    return pa.fixed_shape_tensor(pa.int8(), [1, 2], dim_names=dim_names)


# (id, prepared schema, input schema, options under which the input differs in the hash)
# "all" = every option combination; "meta" = include_metadata=True only; "cols" = ordered_columns
# only; "structs" = ordered_struct_fields only.
_MD = {"k": "1"}
_MD2 = {"k": "2"}
_DIFFERENCES: list[tuple[str, pa.Schema, pa.Schema, str]] = [
    ("column-name", pa.schema([_field("a", pa.int8())]), pa.schema([_field("b", pa.int8())]), "all"),
    ("column-type", pa.schema([_field("a", pa.int8())]), pa.schema([_field("a", pa.int16())]), "all"),
    ("column-nullability", pa.schema([_field("a", pa.int8())]), pa.schema([_field("a", pa.int8(), False)]), "all"),
    ("extra-column", pa.schema([_field("a", pa.int8())]),
     pa.schema([_field("a", pa.int8()), _field("b", pa.int8())]), "all"),
    ("struct-child-nullability", pa.schema([_field("s", pa.struct([_field("x", pa.int8())]))]),
     pa.schema([_field("s", pa.struct([_field("x", pa.int8(), False)]))]), "all"),
    ("list-element-nullability", pa.schema([_field("l", pa.list_(_field("item", pa.int8())))]),
     pa.schema([_field("l", pa.list_(_field("item", pa.int8(), False)))]), "all"),
    ("list-vs-fixed-size-list", pa.schema([_field("l", pa.list_(pa.int8()))]),
     pa.schema([_field("l", pa.list_(pa.int8(), 2))]), "all"),
    ("fixed-size-list-size", pa.schema([_field("l", pa.list_(pa.int8(), 2))]),
     pa.schema([_field("l", pa.list_(pa.int8(), 3))]), "all"),
    ("timestamp-zone", pa.schema([_field("t", pa.timestamp("us"))]),
     pa.schema([_field("t", pa.timestamp("us", "UTC"))]), "all"),
    ("decimal-scale", pa.schema([_field("d", pa.decimal128(10, 2))]),
     pa.schema([_field("d", pa.decimal128(10, 3))]), "all"),
    ("column-order", pa.schema([_field("a", pa.int8()), _field("b", pa.int8())]),
     pa.schema([_field("b", pa.int8()), _field("a", pa.int8())]), "cols"),
    ("struct-field-order", pa.schema([_field("s", pa.struct([_field("x", pa.int8()), _field("y", pa.int8())]))]),
     pa.schema([_field("s", pa.struct([_field("y", pa.int8()), _field("x", pa.int8())]))]), "structs"),
    ("column-metadata", pa.schema([_field("a", pa.int8(), metadata=_MD)]),
     pa.schema([_field("a", pa.int8(), metadata=_MD2)]), "meta"),
    ("column-metadata-removed", pa.schema([_field("a", pa.int8(), metadata=_MD)]),
     pa.schema([_field("a", pa.int8())]), "meta"),
    ("schema-metadata", pa.schema([_field("a", pa.int8())], metadata=_MD),
     pa.schema([_field("a", pa.int8())], metadata=_MD2), "meta"),
    ("struct-child-metadata", pa.schema([_field("s", pa.struct([_field("x", pa.int8(), metadata=_MD)]))]),
     pa.schema([_field("s", pa.struct([_field("x", pa.int8(), metadata=_MD2)]))]), "meta"),
    ("list-element-metadata", pa.schema([_field("l", pa.list_(_field("item", pa.int8(), metadata=_MD)))]),
     pa.schema([_field("l", pa.list_(_field("item", pa.int8(), metadata=_MD2)))]), "meta"),
    ("fixed-size-list-element-metadata", pa.schema([_field("l", pa.list_(_field("item", pa.int8(), metadata=_MD), 2))]),
     pa.schema([_field("l", pa.list_(_field("item", pa.int8(), metadata=_MD2), 2))]), "meta"),
    # pyarrow's Schema.equals(check_metadata=True) ignores this difference (pyarrow 26).
    ("dictionary-value-struct-child-metadata",
     pa.schema([_field("d", pa.dictionary(pa.int8(), pa.struct([_field("x", pa.int8(), metadata=_MD)])))]),
     pa.schema([_field("d", pa.dictionary(pa.int8(), pa.struct([_field("x", pa.int8(), metadata=_MD2)])))]), "meta"),
    # pyarrow's default ExtensionType equality ignores the serialized parameter.
    ("python-extension-parameter", pa.schema([_field("u", ra.UnitType("m"))]),
     pa.schema([_field("u", ra.UnitType("s"))]), "meta"),
    ("canonical-extension-parameter", pa.schema([_field("t", _tensor(None))]),
     pa.schema([_field("t", _tensor(["r", "c"]))]), "meta"),
    ("extension-vs-storage", pa.schema([_field("u", ra.UnitType("m"))]),
     pa.schema([_field("u", pa.int64())]), "meta"),
    ("extension-storage-child-metadata",
     pa.schema([_field("t", _tensor(None))]),
     pa.schema([_field("t", pa.list_(_field("item", pa.int8(), metadata=_MD), 2))]), "meta"),
]


def _differs_under(kind: str, options: dict[str, bool]) -> bool:
    return {
        "all": True,
        "meta": options["include_metadata"],
        "cols": options["ordered_columns"],
        "structs": options["ordered_struct_fields"],
    }[kind]


def _empty(schema: pa.Schema) -> pa.Table:
    rng = random.Random(0)
    return pa.Table.from_arrays([ra.random_array(f.type, 0, f.nullable, rng) for f in schema], schema=schema)


def _one_row(schema: pa.Schema, seed: int = 0) -> pa.Table:
    rng = random.Random(seed)
    columns = [ra.random_array(f.type, 1, f.nullable, rng) for f in schema]
    return pa.Table.from_arrays(columns, schema=schema)


@pytest.mark.parametrize("options", OPTION_SETS, ids=_OPTION_IDS)
@pytest.mark.parametrize(("case", "prepared_schema", "input_schema", "kind"), _DIFFERENCES,
                         ids=[d[0] for d in _DIFFERENCES])
def test_same_schema_rule(case: str, prepared_schema: pa.Schema, input_schema: pa.Schema, kind: str,
                          options: dict[str, bool]) -> None:
    """Refused exactly when the input's schema hashes differently under the prepared options;
    otherwise accepted, hashing byte-identically to the one-shot path."""
    prepared = ArrowDigester.prepare(prepared_schema, **options)
    differs = ArrowDigester.hash_schema(input_schema, **options) != prepared.schema_digest
    assert differs == _differs_under(kind, options), case
    for table in (_empty(input_schema), _one_row(input_schema)):
        batch = pa.RecordBatch.from_arrays([c.combine_chunks() for c in table.columns], schema=input_schema)
        if differs:
            with pytest.raises(ValueError, match=_TABLE_MISMATCH):
                prepared.hash_table(table)
            with pytest.raises(ValueError, match=_BATCH_MISMATCH):
                prepared.hash_record_batch(batch)
            d = prepared.digester()
            with pytest.raises(ValueError, match=_STREAM_MISMATCH):
                d.update(batch)
            assert d.finalize() == prepared.hash_table(_empty(prepared_schema))  # left unchanged
        else:
            _assert_same_as_one_shot(table, options, prepared)


# Hash-equivalent but not identical schemas: accepted (today's streaming rule) under every option set
# where they hash alike, through the analysis fallback.
_EQUIVALENT: list[tuple[str, pa.Schema, pa.Schema]] = [
    ("string-vs-large-string", pa.schema([_field("a", pa.string())]), pa.schema([_field("a", pa.large_string())])),
    ("binary-vs-large-binary", pa.schema([_field("a", pa.binary())]), pa.schema([_field("a", pa.large_binary())])),
    ("list-vs-large-list", pa.schema([_field("l", pa.list_(pa.int8()))]), pa.schema([_field("l", pa.large_list(pa.int8()))])),
    ("dictionary-vs-plain", pa.schema([_field("a", pa.string())]),
     pa.schema([_field("a", pa.dictionary(pa.int32(), pa.string()))])),
    ("dictionary-index-type", pa.schema([_field("a", pa.dictionary(pa.int8(), pa.string()))]),
     pa.schema([_field("a", pa.dictionary(pa.int32(), pa.string()))])),
    ("list-element-name", pa.schema([_field("l", pa.list_(_field("item", pa.int8())))]),
     pa.schema([_field("l", pa.list_(_field("element", pa.int8())))])),
    ("metadata-key-order", pa.schema([_field("a", pa.int8(), metadata={"x": "1", "y": "2"})], metadata={"p": "1", "q": "2"}),
     pa.schema([_field("a", pa.int8(), metadata={"y": "2", "x": "1"})], metadata={"q": "2", "p": "1"})),
]


@pytest.mark.parametrize("options", OPTION_SETS, ids=_OPTION_IDS)
@pytest.mark.parametrize(("case", "prepared_schema", "input_schema"), _EQUIVALENT, ids=[e[0] for e in _EQUIVALENT])
def test_hash_equivalent_schemas_are_accepted(case: str, prepared_schema: pa.Schema, input_schema: pa.Schema,
                                              options: dict[str, bool]) -> None:
    assert ArrowDigester.hash_schema(input_schema, **options) == ArrowDigester.hash_schema(prepared_schema, **options)
    prepared = ArrowDigester.prepare(prepared_schema, **options)
    for table in (_empty(input_schema), _one_row(input_schema)):
        _assert_same_as_one_shot(table, options, prepared)


@pytest.mark.parametrize("options", OPTION_SETS, ids=_OPTION_IDS)
def test_seeded_schema_pairs_accepted_iff_schema_digests_equal(options: dict[str, bool]) -> None:
    """Random pairs, many of them hash-equivalent after dropping metadata or reordering."""
    rng = random.Random(1177)
    accepted = refused = 0
    for _ in range(150):
        a = ra.random_schema(rng)
        b = rng.choice([
            ra.random_schema(rng),
            a.remove_metadata(),
            pa.schema(list(reversed(list(a))), metadata=a.metadata),
            pa.schema([f.remove_metadata() if rng.random() < 0.5 else f for f in a], metadata=a.metadata),
            a,
        ])
        prepared = ArrowDigester.prepare(a, **options)
        table = ra.random_table(rng, b)
        if ArrowDigester.hash_schema(b, **options) == prepared.schema_digest:
            accepted += 1
            assert prepared.hash_table(table) == ArrowDigester.hash_table(table, **options)
        else:
            refused += 1
            with pytest.raises(ValueError, match=_TABLE_MISMATCH):
                prepared.hash_table(table)
    assert accepted > 20 and refused > 20


def test_ipc_fast_path_is_deterministic_for_equal_schemas() -> None:
    """The fast path compares Arrow IPC schema bytes; equal schemas built in different ways
    (pandas, a JSON-like round trip, IPC) serialize identically or fall back to the full check."""
    table = _simple_table()
    schema = table.schema
    assert schema.serialize().to_pybytes() == schema.serialize().to_pybytes()
    via_ipc = pa.ipc.read_schema(pa.py_buffer(schema.serialize().to_pybytes()))
    rebuilt = pa.schema([pa.field(f.name, f.type, f.nullable, f.metadata) for f in schema], metadata=schema.metadata)
    for options in OPTION_SETS:
        prepared = ArrowDigester.prepare(schema, **options)
        for other in (via_ipc, rebuilt):
            assert other.serialize().to_pybytes() == schema.serialize().to_pybytes()
            _assert_same_as_one_shot(pa.Table.from_batches(table.to_batches(), schema=other), options, prepared)


# ---------------------------------------------------------------------------
# Differential: golden vectors, seeded random inputs
# ---------------------------------------------------------------------------

_GOLDEN = Path(__file__).parent / "golden" / "format2_v0.5.json"


@cache
def _golden_record_vectors() -> list[dict[str, Any]]:
    vectors = json.loads(_GOLDEN.read_text(encoding="utf-8"))["vectors"]
    return [v for v in vectors if v["method"] in ("hash_schema", "hash_record_batch", "hash_stream")]


def _decode(vector: dict[str, Any]) -> tuple[pa.Schema, list[pa.RecordBatch]]:
    reader = pa.ipc.open_stream(io.BytesIO(base64.b64decode(vector["ipc_b64"])))
    return reader.schema, list(reader)


def _outcome(call: Any, *args: Any, **kwargs: Any) -> str:
    try:
        return call(*args, **kwargs).hex()
    except (ValueError, TypeError, NotImplementedError) as e:
        return f"raises {type(e).__name__}: {e}"


def _prepared_schema_digest(schema: pa.Schema, **options: bool) -> bytes:
    return ArrowDigester.prepare(schema, **options).schema_digest


def _prepared_hash_table(table: pa.Table, **options: bool) -> bytes:
    return ArrowDigester.prepare(table.schema, **options).hash_table(table)


def test_golden_vectors_prepared_equal_one_shot_for_every_option_set() -> None:
    """Every record/schema vector of the Rust golden file, under all 8 option combinations: the
    prepared path equals the one-shot path (and the Rust hash for the vector's own options)."""
    vectors = _golden_record_vectors()
    assert len(vectors) > 300
    for vector in vectors:
        schema, batches = _decode(vector)
        own = {"include_metadata": vector["include_metadata"], "ordered_columns": vector["ordered_columns"],
               "ordered_struct_fields": vector["ordered_struct_fields"]}
        for options in OPTION_SETS:
            prepared_or_error = _outcome(_prepared_schema_digest, schema, **options)
            assert prepared_or_error == _outcome(ArrowDigester.hash_schema, schema, **options)
            if prepared_or_error.startswith("raises"):
                continue
            prepared = ArrowDigester.prepare(schema, **options)
            if vector["method"] == "hash_schema":
                got = prepared.schema_digest.hex()
            else:
                table = pa.Table.from_batches(batches, schema=schema)
                got = _outcome(prepared.hash_table, table)
                assert got == _outcome(ArrowDigester.hash_table, table, **options), vector["id"]
                assert got == _outcome(_prepared_stream, prepared, batches), vector["id"]
                assert got == _outcome(_one_shot_stream, schema, batches, **options), vector["id"]
                if len(batches) == 1:
                    assert got == _outcome(prepared.hash_record_batch, batches[0]), vector["id"]
            if options == own:
                assert got == vector["expected_hash"], vector["id"]


@pytest.mark.parametrize("chunk", range(8))
def test_seeded_random_inputs_prepared_equal_one_shot(chunk: int) -> None:
    for seed in range(chunk * 40, chunk * 40 + 40):
        rng = random.Random(seed)
        table = ra.random_table(rng)
        for options in OPTION_SETS:
            one_shot = _outcome(ArrowDigester.hash_table, table, **options)
            if one_shot.startswith("raises"):
                assert _outcome(_prepared_hash_table, table, **options) == one_shot
                continue
            _assert_same_as_one_shot(table, options)
