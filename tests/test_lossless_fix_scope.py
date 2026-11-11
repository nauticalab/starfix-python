"""Scope of the lossless-layout fix (ITL-898 developer ruling, 2026-11-11).

The lossless layout (byte-layout spec 9.5 / 9.5.1) becomes the default. Under variant (a)
(version prefix unchanged) hashes of *unaffected* inputs must stay byte-identical. Rule:

    a type is AFFECTED iff it contains a Boolean, a nullable struct, or a list whose
    element is a nullable list or a nullable struct (dictionaries are looked through).

``hash_schema`` is never affected (the layout only changes data entries). For
``hash_array`` the root counts as nullable iff the array contains a null.

"Affected" is a property of the type: some values of an affected type hash the same in
both layouts (for example a single Boolean value packs to the same byte).
"""

from __future__ import annotations

import base64
import io
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _pyarrow_compat import UNREADABLE_HERE

import starfix.arrow_digester as ad
from starfix.arrow_digester import _OPTION_FLAGS, ArrowDigester

_GOLDEN_DIR = Path(__file__).parent / "golden"


def _strip_dictionary(t: pa.DataType) -> pa.DataType:
    while pa.types.is_dictionary(t):
        t = t.value_type
    return t


def affected(t: pa.DataType, nullable: bool) -> bool:
    t = _strip_dictionary(t)
    if pa.types.is_boolean(t):
        return True
    if pa.types.is_struct(t):
        return nullable or any(affected(t.field(i).type, t.field(i).nullable) for i in range(t.num_fields))
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        element = t.value_field
        et = _strip_dictionary(element.type)
        if element.nullable and (pa.types.is_struct(et) or pa.types.is_list(et) or pa.types.is_large_list(et)):
            return True
        return affected(element.type, element.nullable)
    return False


def _vectors() -> list[dict[str, Any]]:
    out = []
    for path in sorted(_GOLDEN_DIR.glob("*.json")):
        with path.open(encoding="utf-8") as f:
            for v in json.load(f)["vectors"]:
                # Only vectors without the (retired) third flag describe the default layout.
                if v["id"] in UNREADABLE_HERE:
                    continue  # this pyarrow cannot decode the vector (see _pyarrow_compat)
                if not v.get("nested_validity", False):
                    out.append({**v, "_file": path.name})
    return out


def _hash(vector: dict[str, Any]) -> str:
    reader = pa.ipc.open_stream(io.BytesIO(base64.b64decode(vector["ipc_b64"])))
    schema, batches = reader.schema, list(reader)
    options = {k: vector.get(k, False) for k in _OPTION_FLAGS if k != "nested_validity"}
    method = vector["method"]
    if method == "hash_schema":
        return ArrowDigester.hash_schema(schema, include_metadata=vector["include_metadata"], **options).hex()
    if method == "hash_array":
        return ArrowDigester.hash_array(batches[0].column(0), **options).hex()
    d = ArrowDigester(schema, include_metadata=vector["include_metadata"], **options)
    for b in batches:
        d.update(b)
    return d.finalize().hex()


def _is_affected(vector: dict[str, Any]) -> bool:
    if vector["method"] == "hash_schema":
        return False
    reader = pa.ipc.open_stream(io.BytesIO(base64.b64decode(vector["ipc_b64"])))
    if vector["method"] == "hash_array":
        arr = next(iter(reader)).column(0)
        return affected(arr.type, arr.null_count > 0)
    return any(affected(f.type, f.nullable) for f in reader.schema)


@pytest.mark.parametrize("vector", _vectors(), ids=lambda v: f"{v['_file']}:{v['id']}")
def test_unaffected_inputs_hash_identically_under_lossless_layout(vector, monkeypatch) -> None:
    if _is_affected(vector):
        return  # affected inputs may move; their new values are pinned by the Rust golden file
    try:
        before = _hash(vector)
    except NotImplementedError:
        return  # types the default serializer rejects (see test_known_divergences.py)
    monkeypatch.setattr(ad, "_LOSSLESS_BY_DEFAULT", True)
    assert _hash(vector) == before, f"unaffected vector {vector['id']} moved under the lossless layout"


@pytest.mark.parametrize(
    "dt, nullable, expected",
    [
        (pa.int32(), True, False),
        (pa.bool_(), False, True),
        (pa.dictionary(pa.int8(), pa.utf8()), True, False),
        (pa.struct([pa.field("a", pa.int32())]), False, False),
        (pa.struct([pa.field("a", pa.int32())]), True, True),
        (pa.struct([pa.field("s", pa.struct([pa.field("a", pa.int8())]), nullable=True)]), False, True),
        (pa.list_(pa.field("item", pa.int32(), nullable=True)), True, False),
        (pa.list_(pa.field("item", pa.list_(pa.int32()), nullable=True)), False, True),
        (pa.list_(pa.field("item", pa.list_(pa.int32()), nullable=False)), True, False),
        (pa.list_(pa.field("item", pa.struct([pa.field("a", pa.int8())]), nullable=True)), False, True),
        (pa.list_(pa.field("item", pa.struct([pa.field("a", pa.int8())]), nullable=False)), True, False),
    ],
)
def test_affected_rule(dt, nullable, expected) -> None:
    assert affected(dt, nullable) is expected
