"""Known default-mode divergences from the Rust starfix implementation (ITL-898).

Each test asserts the Rust value and is marked ``xfail(strict=True)``: it fails today
because starfix-python's released default-mode (no options) output differs from Rust,
and it turns into a hard failure the day the default output is changed to match, so the
marker must then be removed deliberately. Changing a default-mode output moves released
hashes and is the developer's decision; it is not done in ITL-898.

With any hashing option on (``flags != 0``) Python already uses the Rust form for both
cases, and the golden vectors in ``tests/golden/ordered_v0.5.json`` pin that.
"""

from __future__ import annotations

import base64
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


# Rust: hash_array of StructArray [{"é": 1}], child Int32 non-null, no options
# (measured on the starfix ITL-898 branch; serde_json writes "é" as raw UTF-8).
_RUST_NON_ASCII_STRUCT_ARRAY = "000001e445fa3cdc5fe37df7cb3de55cf94dcee2a8990bb8dd46fb5c3b00398f8cfb2c"


@pytest.mark.xfail(
    strict=True,
    reason="Default hash_array type JSON escapes non-ASCII (\\u00e9); Rust writes raw UTF-8",
)
def test_default_hash_array_non_ascii_name_matches_rust() -> None:
    arr = pa.StructArray.from_arrays(
        [pa.array([1], pa.int32())], fields=[pa.field("é", pa.int32(), nullable=False)]
    )
    assert ArrowDigester.hash_array(arr).hex() == _RUST_NON_ASCII_STRUCT_ARRAY
