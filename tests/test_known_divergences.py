"""Known default-mode divergences from the Rust starfix implementation (ITL-898).

Each test asserts the Rust value and is marked ``xfail(strict=True)``: it fails today
because starfix-python's released default-mode (no options) output differs from Rust,
and it turns into a hard failure the day the default output is changed to match, so the
marker must then be removed deliberately. Changing a default-mode output moves released
hashes and is the developer's decision; it is not done in ITL-898.

With any hashing option on (``flags != 0``) Python already uses the Rust form for the
two JSON cases and checks every streamed batch, and the golden vectors in
``tests/golden/ordered_v0.5.json`` pin that.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
from pathlib import Path

import pyarrow as pa
import pytest

from starfix.arrow_digester import ArrowDigester

_GOLDEN = Path(__file__).parent / "golden" / "ordered_v0.5.json"


def _golden_vector(vector_id: str) -> dict:
    with _GOLDEN.open(encoding="utf-8") as f:
        return next(v for v in json.load(f)["vectors"] if v["id"] == vector_id)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Default Map JSON omits the map's value type and keys_sorted: Python emits "
        '{"Map":[{"data_type":<key type>,"name":"entries","nullable":false},false]}, '
        "Rust emits the entries field (Struct[key, value]) and keys_sorted"
    ),
)
@pytest.mark.parametrize("vector_id", ["map_struct_ab_schema_f0", "map_struct_ba_schema_f0"])
def test_default_map_schema_matches_rust(vector_id: str) -> None:
    vector = _golden_vector(vector_id)
    schema = pa.ipc.open_stream(io.BytesIO(base64.b64decode(vector["ipc_b64"]))).schema
    assert ArrowDigester.hash_schema(schema).hex() == vector["expected_hash"]


def test_default_map_schema_ignores_value_type_today() -> None:
    """Documents the consequence: the default schema hash ignores a map's value type."""
    a = pa.schema([pa.field("m", pa.map_(pa.utf8(), pa.int8()))])
    b = pa.schema([pa.field("m", pa.map_(pa.utf8(), pa.struct([pa.field("x", pa.int64())])))])
    assert ArrowDigester.hash_schema(a) == ArrowDigester.hash_schema(b)
    assert ArrowDigester.hash_schema(a, ordered_struct_fields=True) != ArrowDigester.hash_schema(
        b, ordered_struct_fields=True
    )


@pytest.mark.xfail(
    strict=True,
    reason="Default hash_array type JSON escapes non-ASCII (\\u00e9); Rust writes raw UTF-8",
)
@pytest.mark.parametrize("vector_id", ["names_array_struct_f0", "names_array_struct_int8_f0"])
def test_default_hash_array_non_ascii_name_matches_rust(vector_id: str) -> None:
    vector = _golden_vector(vector_id)
    batch = next(iter(pa.ipc.open_stream(io.BytesIO(base64.b64decode(vector["ipc_b64"])))))
    assert ArrowDigester.hash_array(batch.column(0)).hex() == vector["expected_hash"]


# Spec Section 2.1 Map example (literal JSON and hash from the spec text).
_SPEC_MAP_JSON = (
    '{"m":{"data_type":{"Map":[{"data_type":{"Struct":[{"data_type":"LargeUtf8","name":"key",'
    '"nullable":false},{"data_type":{"Struct":[{"data_type":"Int32","name":"a","nullable":false},'
    '{"data_type":"Int32","name":"z","nullable":false}]},"name":"value","nullable":true}]},'
    '"name":"entries","nullable":false},false]},"nullable":true}}'
)
_SPEC_MAP_HASH = "000001edad0efdc8086afa34e6633ee4213d2ed803cbd8849e7d062f882f698bdcf897"


def _spec_map_schema() -> pa.Schema:
    value = pa.struct([pa.field("z", pa.int32(), nullable=False), pa.field("a", pa.int32(), nullable=False)])
    return pa.schema([pa.field("m", pa.map_(pa.utf8(), value), nullable=True)])


def test_spec_map_example_is_self_consistent() -> None:
    assert "000001" + hashlib.sha256(_SPEC_MAP_JSON.encode()).hexdigest() == _SPEC_MAP_HASH


@pytest.mark.xfail(strict=True, reason="Default Map JSON differs from the spec Section 2.1 form")
def test_default_map_spec_example() -> None:
    assert ArrowDigester.hash_schema(_spec_map_schema()).hex() == _SPEC_MAP_HASH


def test_map_spec_form_used_with_options() -> None:
    """With any option on, Phase 1 uses the spec Section 2.1 Map form (here under the tag)."""
    tag = b"\x00starfix-options-v1\x04"
    expected = "000001" + hashlib.sha256(tag + _SPEC_MAP_JSON.encode()).hexdigest()
    assert ArrowDigester.hash_schema(_spec_map_schema(), nested_validity=True).hex() == expected


# Spec Section 9.7: with flags == 0, Rust also checks every batch (Phase 1 JSON equality,
# plus the Phase 2 document with include_metadata). Python has never checked at flags == 0.


@pytest.mark.xfail(strict=True, reason="Python does not check batches against the schema with no options")
def test_default_update_rejects_mismatched_batch() -> None:
    d = ArrowDigester(pa.schema([pa.field("a", pa.int32())]))
    with pytest.raises(ValueError):
        d.update(pa.record_batch([pa.array([1], pa.int64())], names=["a"]))


@pytest.mark.xfail(strict=True, reason="Python does not check batches against the schema with no options")
def test_default_update_rejects_metadata_mismatch_with_include_metadata() -> None:
    d = ArrowDigester(pa.schema([pa.field("a", pa.int32())]), include_metadata=True)
    other = pa.schema([pa.field("a", pa.int32(), metadata={"k": "v"})])
    with pytest.raises(ValueError):
        d.update(pa.record_batch([pa.array([1], pa.int32())], schema=other))
