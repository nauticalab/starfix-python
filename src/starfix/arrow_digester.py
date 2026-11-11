"""Pure-Python implementation of the starfix Arrow logical hasher.

Implements the byte-layout specification defined in the starfix Rust crate
(``nauticalab/starfix docs/byte-layout-spec.md``).
"""

from __future__ import annotations

import hashlib
import json
import struct
from collections import OrderedDict
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa


class _Hasher(Protocol):
    """Minimal protocol for a hash accumulator (e.g. ``hashlib.sha256()``)."""

    def update(self, data: bytes | bytearray, /) -> None: ...


# Hash format version (developer decision 2026-11-11: bump it). Format 1 is the
# v0.4.0 layout. Format 2 (pending the regenerated Rust spec and golden file) is the
# corrected lossless layout with every default-mode alignment below; switching to it
# is this one line. The version prefix is the 3-byte big-endian format number.
_FORMAT_VERSION = 1
VERSION_BYTES = _FORMAT_VERSION.to_bytes(3, "big")
DELIMITER = "/"

_FORMAT_2 = _FORMAT_VERSION >= 2

# ---------------------------------------------------------------------------
# Default-mode changes that format 2 brings. Each is a separate switch so that it
# can be tested on its own: True applies the Rust/spec behaviour with no options on
# (it already applies whenever an option is on). tests/test_known_divergences.py
# checks each switch against the Rust values and pins format 1 as strict xfails.
# ---------------------------------------------------------------------------

# Lossless layout (spec 9.5 / 9.5.1): developer ruling 2026-11-11, the only layout in format 2.
_LOSSLESS_BY_DEFAULT = _FORMAT_2
# Map schema JSON: entries field (Struct[key, value]) and keys_sorted (spec 2.1).
_FIX_DEFAULT_MAP_JSON = _FORMAT_2
# hash_array type JSON: non-ASCII written raw, as serde_json does (spec 2.1).
_FIX_DEFAULT_TYPE_JSON_UTF8 = _FORMAT_2
# Null / Interval / Utf8View / BinaryView schema forms (spec 2.1); format 1 raises.
_FIX_DEFAULT_TYPE_FORMS = _FORMAT_2
# Phase 2 metadata paths (2.2.1): map entries m/, key m//key, value m//value, and
# dictionaries looked through.
_FIX_DEFAULT_MAP_METADATA_PATHS = _FORMAT_2
# Streaming: reject a batch whose schema differs from the digester's (spec 9.7).
_FIX_DEFAULT_BATCH_CHECK = _FORMAT_2

# Hashing options (spec Section 9.1): keyword name -> bit of the flags byte.
# This table is the single place that names the options; the keyword arguments of
# the public API must use exactly these names.
_OPTION_FLAGS: dict[str, int] = {
    "ordered_columns": 0x01,
    "ordered_struct_fields": 0x02,
    "nested_validity": 0x04,
}

# Options tag prefix (spec Section 9.2); the flags byte is appended.
_OPTIONS_TAG_PREFIX = b"\x00starfix-options-v1"

_ORDERED_COLUMNS = _OPTION_FLAGS["ordered_columns"]
_ORDERED_STRUCT_FIELDS = _OPTION_FLAGS["ordered_struct_fields"]
_NESTED_VALIDITY = _OPTION_FLAGS["nested_validity"]

_SCHEMA_MISMATCH = "Record batch schema does not match ArrowDigester schema"


def _flags(ordered_columns: bool, ordered_struct_fields: bool, nested_validity: bool) -> int:
    """Build the flags byte of spec Section 9.1 from the option values."""
    return (
        (_ORDERED_COLUMNS if ordered_columns else 0)
        | (_ORDERED_STRUCT_FIELDS if ordered_struct_fields else 0)
        | (_NESTED_VALIDITY if nested_validity else 0)
    )


def _reject_nested_validity(nested_validity: bool) -> None:
    """Format 2 has no third option: its layout is always lossless (spec format 2, §1)."""
    if nested_validity:
        raise TypeError("nested_validity is not an option in hash format 2 (its layout is always lossless)")


def _lossless(flags: int) -> bool:
    """Whether the lossless layout of spec 9.5 / 9.5.1 applies."""
    return _LOSSLESS_BY_DEFAULT or bool(flags & _NESTED_VALIDITY)


def _options_tag(flags: int) -> bytes:
    """Options tag (spec Section 9.2): 0x00 || "starfix-options-v1" || flags (20 bytes)."""
    return _OPTIONS_TAG_PREFIX + bytes([flags])


# ---------------------------------------------------------------------------
# Bit-vector helper (LSB-first packing, matching bitvec<u8, Lsb0>)
# ---------------------------------------------------------------------------

class _BitVec:
    """Minimal LSB-first u8 bit vector compatible with Rust bitvec<u8, Lsb0>.

    Matches Arrow's native validity bitmap layout.
    """

    __slots__ = ("_bytes", "_len")

    def __init__(self) -> None:
        self._bytes = bytearray()
        self._len = 0

    def push(self, bit: bool) -> None:
        byte_idx = self._len >> 3
        bit_idx = self._len & 7  # LSB-first: bit 0 is least significant
        if byte_idx >= len(self._bytes):
            self._bytes.append(0)
        if bit:
            self._bytes[byte_idx] |= 1 << bit_idx
        self._len += 1

    def extend_true(self, count: int) -> None:
        for _ in range(count):
            self.push(True)

    def __len__(self) -> int:
        return self._len

    def raw_bytes(self) -> bytes:
        return bytes(self._bytes)


# ---------------------------------------------------------------------------
# Schema / DataType serialization  (spec Section 2)
# ---------------------------------------------------------------------------

def _data_type_to_value(dt: pa.DataType, flags: int = 0) -> object:
    """Convert a pyarrow DataType to the JSON-compatible value that matches
    the canonical form described in spec Section 2.1.

    Types are normalized: Utf8→LargeUtf8, Binary→LargeBinary, List→LargeList,
    Dictionary→value_type. Struct fields are sorted alphabetically, or kept in
    declaration order when ``flags`` has ``ordered_struct_fields`` (spec
    Section 9.3), at every depth. ``flags == 0`` is the historical form.
    """
    import pyarrow as pa

    # Normalize: Dictionary → recurse on value type
    if pa.types.is_dictionary(dt):
        return _data_type_to_value(dt.value_type, flags)

    if pa.types.is_struct(dt):
        fields = [dt.field(i) for i in range(dt.num_fields)]
        if not flags & _ORDERED_STRUCT_FIELDS:
            # Sort struct fields alphabetically by name
            fields.sort(key=lambda f: f.name)
        fields_json = [_inner_field_to_value(f, flags) for f in fields]
        return {"Struct": fields_json}
    if pa.types.is_list(dt) or pa.types.is_large_list(dt):
        return {"LargeList": _element_type_to_value(dt.value_field, flags)}
    if pa.types.is_fixed_size_list(dt):
        return {"FixedSizeList": [_element_type_to_value(dt.value_field, flags), dt.list_size]}
    if pa.types.is_map(dt):
        if flags or _FIX_DEFAULT_MAP_JSON:
            # The entries field (a struct of key and value) and keys_sorted,
            # as the Rust implementation serializes DataType::Map(entries, sorted).
            return {"Map": [_inner_field_to_value(dt.field(0), flags), dt.keys_sorted]}
        # Historical form (flags == 0), kept byte for byte: it omits the value
        # type and does not match the Rust form (see tests/test_known_divergences.py).
        return {"Map": [_inner_field_to_value(dt.key_field.with_name("entries")), False]}

    # Primitive / leaf types – must match Arrow-Rust serde
    return _primitive_data_type_string(dt, flags)


# Spec Section 2.1 forms of types that the historical (flags == 0) serializer rejects.
# They are used only when an option is on, so default-mode behaviour is unchanged.
_V05_ONLY_TYPE_FORMS = {
    "null": "Null",
    "month_interval": {"Interval": "YearMonth"},
    "day_time_interval": {"Interval": "DayTime"},
    "month_day_nano_interval": {"Interval": "MonthDayNano"},
    "string_view": "Utf8View",
    "binary_view": "BinaryView",
}


def _primitive_data_type_string(dt: pa.DataType, flags: int = 0) -> object:
    """Return the serde_json representation that arrow-rs produces (spec Section 2.1)."""
    import pyarrow as pa

    _simple = {
        pa.bool_(): "Boolean",
        pa.int8(): "Int8",
        pa.uint8(): "UInt8",
        pa.int16(): "Int16",
        pa.uint16(): "UInt16",
        pa.int32(): "Int32",
        pa.uint32(): "UInt32",
        pa.int64(): "Int64",
        pa.uint64(): "UInt64",
        pa.float16(): "Float16",
        pa.float32(): "Float32",
        pa.float64(): "Float64",
        pa.date32(): "Date32",
        pa.date64(): "Date64",
        pa.utf8(): "LargeUtf8",
        pa.large_utf8(): "LargeUtf8",
        pa.binary(): "LargeBinary",
        pa.large_binary(): "LargeBinary",
    }
    if dt in _simple:
        return _simple[dt]

    if pa.types.is_decimal(dt):
        if dt.bit_width == 32:
            return {"Decimal32": [dt.precision, dt.scale]}
        if dt.bit_width == 64:
            return {"Decimal64": [dt.precision, dt.scale]}
        if dt.bit_width == 128:
            return {"Decimal128": [dt.precision, dt.scale]}
        if dt.bit_width == 256:
            return {"Decimal256": [dt.precision, dt.scale]}

    if pa.types.is_time32(dt):
        unit = "Second" if dt.unit == "s" else "Millisecond"
        return {"Time32": unit}
    if pa.types.is_time64(dt):
        unit = "Microsecond" if dt.unit == "us" else "Nanosecond"
        return {"Time64": unit}

    if pa.types.is_timestamp(dt):
        unit_map = {"s": "Second", "ms": "Millisecond", "us": "Microsecond", "ns": "Nanosecond"}
        unit = unit_map[dt.unit]
        if dt.tz is None:
            return {"Timestamp": [unit, None]}
        return {"Timestamp": [unit, dt.tz]}

    if pa.types.is_duration(dt):
        unit_map = {"s": "Second", "ms": "Millisecond", "us": "Microsecond", "ns": "Nanosecond"}
        return {"Duration": unit_map[dt.unit]}

    if pa.types.is_fixed_size_binary(dt):
        return {"FixedSizeBinary": dt.byte_width}

    if (flags or _FIX_DEFAULT_TYPE_FORMS) and str(dt) in _V05_ONLY_TYPE_FORMS:
        return _V05_ONLY_TYPE_FORMS[str(dt)]

    raise NotImplementedError(f"Unsupported data type: {dt}")


def _inner_field_to_value(field: pa.Field, flags: int = 0) -> dict:
    """Convert a field to JSON with name, data_type, and nullable."""
    return {
        "name": field.name,
        "data_type": _data_type_to_value(field.type, flags),
        "nullable": field.nullable,
    }


def _element_type_to_value(field: pa.Field, flags: int = 0) -> dict:
    """Convert a container element field to JSON with only data_type and nullable (no name).

    Used for list and fixed-size list element types, matching Rust ``element_type_to_value``.
    """
    return {
        "data_type": _data_type_to_value(field.type, flags),
        "nullable": field.nullable,
    }


def _sort_json_value(value: object) -> object:
    """Recursively sort JSON object keys (matching Rust ``sort_json_value``)."""
    if isinstance(value, dict):
        return OrderedDict(sorted((k, _sort_json_value(v)) for k, v in value.items()))
    if isinstance(value, list):
        return [_sort_json_value(v) for v in value]
    return value


def _serialized_schema(schema: pa.Schema) -> str:
    fields: dict[str, object] = {}
    for i in range(len(schema)):
        field = schema.field(i)
        value = {
            "data_type": _data_type_to_value(field.type),
            "nullable": field.nullable,
        }
        fields[field.name] = _sort_json_value(value)
    # Sort by field name (BTreeMap ordering)
    sorted_fields = OrderedDict(sorted(fields.items()))
    return json.dumps(sorted_fields, separators=(",", ":"), ensure_ascii=False)


def _phase1_json(schema: pa.Schema, flags: int) -> str:
    """Phase 1 canonical schema JSON for a flags byte (spec Sections 2.1 and 9.3).

    ``flags == 0`` is exactly the default serialization (Section 2.1).
    """
    if flags == 0:
        return _serialized_schema(schema)
    fields = [schema.field(i) for i in range(len(schema))]
    if flags & _ORDERED_COLUMNS:
        # Top level is an array of struct-child-form objects, in schema order.
        doc: object = [_sort_json_value(_inner_field_to_value(f, flags)) for f in fields]
    else:
        doc = OrderedDict(sorted(
            (f.name, _sort_json_value({
                "data_type": _data_type_to_value(f.type, flags),
                "nullable": f.nullable,
            }))
            for f in fields
        ))
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False)


def _type_json(data_type: pa.DataType, flags: int) -> str:
    """``hash_array`` canonical type JSON (spec Sections 6 and 9.4).

    ``flags == 0`` keeps the historical serialization byte for byte. With any
    option on, non-ASCII characters are written as raw UTF-8 (as serde_json does).
    """
    value = _sort_json_value(_data_type_to_value(data_type, flags))
    if flags == 0 and not _FIX_DEFAULT_TYPE_JSON_UTF8:
        return json.dumps(value, separators=(",", ":"))
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Node-path rejection  (spec Section 9.6, any option on)
# ---------------------------------------------------------------------------

def _join(parent: str, name: str) -> str:
    return f"{parent}{DELIMITER}{name}" if parent else name


def _claim(seen: set[str], path: str) -> None:
    if path in seen:
        raise ValueError(
            f"Ambiguous node path {path!r}: two schema nodes share one path "
            "(duplicate names, or a name containing '/' that aliases another node); "
            "rejected when any hashing option is on"
        )
    seen.add(path)


def _walk_node_paths(data_type: pa.DataType, path: str, seen: set[str]) -> None:
    import pyarrow as pa

    t = data_type
    while pa.types.is_dictionary(t):
        t = t.value_type
    if pa.types.is_struct(t):
        for i in range(t.num_fields):
            child = t.field(i)
            child_path = _join(path, child.name)
            _claim(seen, child_path)
            _walk_node_paths(child.type, child_path, seen)
    elif pa.types.is_map(t):
        # The element of a map is its entries struct (key, value).
        element_path = f"{path}{DELIMITER}"
        _claim(seen, element_path)
        _walk_node_paths(t.field(0).type, element_path, seen)
    elif pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        element_path = f"{path}{DELIMITER}"
        _claim(seen, element_path)
        _walk_node_paths(t.value_field.type, element_path, seen)


def _check_schema_node_paths(schema: pa.Schema) -> None:
    """Raise ``ValueError`` if two nodes of ``schema`` share a node path (spec 9.6)."""
    seen: set[str] = set()
    for i in range(len(schema)):
        field = schema.field(i)
        _claim(seen, field.name)
        _walk_node_paths(field.type, field.name, seen)


def _check_type_node_paths(data_type: pa.DataType) -> None:
    """Raise ``ValueError`` if two nodes of an array type share a node path (spec 9.6)."""
    seen: set[str] = set()
    _claim(seen, "")
    _walk_node_paths(data_type, "", seen)


# ---------------------------------------------------------------------------
# Metadata helpers  (Phase 2 schema hashing — PLT-1734)
# ---------------------------------------------------------------------------

def _sort_metadata(metadata) -> dict[str, str]:
    """Decode PyArrow's bytes-keyed/valued metadata into a sorted str-keyed/valued dict.

    Returns an empty dict when metadata is None or empty.
    Arrow IPC spec guarantees metadata keys and values are valid UTF-8.
    The str fallback in the key/value decoding is defensive for direct callers.
    """
    if not metadata:
        return {}
    return dict(sorted(
        (k.decode("utf-8") if isinstance(k, bytes) else k,
         v.decode("utf-8") if isinstance(v, bytes) else v)
        for k, v in metadata.items()
    ))


def _collect_nested_field_metadata(
    field, path: str, flags: int = 0
) -> dict[str, dict[str, str]]:
    """Recursively collect metadata from a field and its nested children.

    Returns a dict sorted by path: {path: sorted_metadata_dict}.
    Only fields with non-empty metadata are included.

    Path convention (matches the data-hashing BTreeMap paths):
    - Struct children:                          "{parent_path}/{child_name}"
    - List/LargeList/FixedSizeList/Map element: "{parent_path}/"  (trailing slash, no element name)

    For Map types with ``flags == 0`` (historical form), only the value/item field
    is traversed, at "{parent_path}/"; Map key fields are skipped. With any option
    on (spec Section 2.2.1), a map's element is its entries field: entries at
    "{parent_path}/", key at "{parent_path}//key", value at "{parent_path}//value".
    """
    import pyarrow as pa

    result: dict[str, dict[str, str]] = {}

    if field.metadata:
        result[path] = _sort_metadata(field.metadata)

    # Spec 2.2.1 paths (options on) are the 9.6 node paths: dictionaries are looked
    # through and a map's element is its entries field. Format 1 does neither.
    rust_paths = flags or _FIX_DEFAULT_MAP_METADATA_PATHS
    t = field.type
    while rust_paths and pa.types.is_dictionary(t):
        t = t.value_type

    if pa.types.is_struct(t):
        for i in range(t.num_fields):
            child = t.field(i)
            child_path = f"{path}{DELIMITER}{child.name}"
            result.update(_collect_nested_field_metadata(child, child_path, flags))
    elif (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
    ):
        element_path = f"{path}{DELIMITER}"
        result.update(_collect_nested_field_metadata(t.value_field, element_path, flags))
    elif pa.types.is_map(t):
        element_path = f"{path}{DELIMITER}"
        element = t.field(0) if rust_paths else t.item_field
        result.update(_collect_nested_field_metadata(element, element_path, flags))

    return dict(sorted(result.items()))


def _update_metadata_hash(hasher: _Hasher, schema: pa.Schema, flags: int = 0) -> None:
    """Feed Phase 2 metadata JSON into ``hasher`` (only when metadata is present).

    Builds a compact JSON string of the form:
      {"fields": {"<path>": {"<key>": "<val>", ...}, ...}, "schema": {"<key>": "<val>", ...}}

    ``"fields"`` is omitted when no field (at any nesting level) has metadata.
    ``"schema"`` is omitted when the schema has no metadata.
    Nothing is written when both would be absent (empty-metadata invariant).
    """
    import pyarrow as pa

    # Collect all field metadata recursively.
    # _collect_nested_field_metadata returns per-subtree sorted paths, but
    # cross-field order depends on schema field order, so we sort globally below.
    all_field_meta: dict[str, dict[str, str]] = {}
    for i in range(len(schema)):
        field = schema.field(i)
        all_field_meta.update(_collect_nested_field_metadata(field, field.name, flags))

    meta_doc: dict[str, object] = {}
    if all_field_meta:
        meta_doc["fields"] = dict(sorted(all_field_meta.items()))
    if schema.metadata:
        meta_doc["schema"] = _sort_metadata(schema.metadata)

    if meta_doc:
        hasher.update(json.dumps(meta_doc, separators=(",", ":"), ensure_ascii=False).encode())


class _BytesCollector:
    """A ``_Hasher`` that records its input (used to compare Phase 2 documents)."""

    def __init__(self) -> None:
        self.data = bytearray()

    def update(self, data: bytes | bytearray, /) -> None:
        self.data += data


def _schema_key(schema: pa.Schema, include_metadata: bool, flags: int) -> tuple[str, bytes]:
    """Everything a schema contributes to the schema digest (spec 9.7 check key)."""
    phase2 = _BytesCollector()
    if include_metadata:
        _update_metadata_hash(phase2, schema, flags)
    return _phase1_json(schema, flags), bytes(phase2.data)


def _hash_schema(schema: pa.Schema, include_metadata: bool = False, flags: int = 0) -> bytes:
    h = hashlib.sha256()
    if flags:
        # Spec Section 9.2: tag, Phase 1 under flags, then Phase 2 unchanged.
        h.update(_options_tag(flags))
        h.update(_phase1_json(schema, flags).encode())
    else:
        h.update(_serialized_schema(schema).encode())
    if include_metadata:
        _update_metadata_hash(h, schema, flags)
    return h.digest()


# ---------------------------------------------------------------------------
# DigestBufferType  (spec Section 3: null_bits, structural, data)
#
# Each entry is a 3-tuple: (BitVec|None, sha256|None, sha256|None)
#   [0] null_bits   – present when nullable
#   [1] structural  – present for list entries
#   [2] data        – present for leaf and list-leaf entries
# ---------------------------------------------------------------------------

def _new_data_only(nullable: bool) -> tuple:
    """Leaf field entry (spec Section 3 — data-only or validity+data)."""
    return (_BitVec() if nullable else None, None, hashlib.sha256())


def _new_structural_only(nullable: bool) -> tuple:
    """List-level entry whose value is a struct or nested list (spec Section 3)."""
    return (_BitVec() if nullable else None, hashlib.sha256(), None)


def _new_list_leaf(nullable: bool) -> tuple:
    """List-level entry whose value is a leaf type (spec Section 3)."""
    return (_BitVec() if nullable else None, hashlib.sha256(), hashlib.sha256())


def _new_validity_only() -> tuple:
    """Nullable parent entry — just null_bits, no structural or data (spec Section 3)."""
    return (_BitVec(), None, None)


# ---------------------------------------------------------------------------
# Type decomposition into BTreeMap entries (spec Sections 3.4, 3.5)
# ---------------------------------------------------------------------------

def _extract_type_entries(
    data_type: pa.DataType,
    nullable: bool,
    path: str,
    out: dict[str, tuple],
    nested_validity: bool = False,
) -> None:
    """Recursively decompose a data type into BTreeMap entries.

    This implements the recursive decomposition described in spec Section 3:
    - Structs are transparent — no entry, recurse into sorted children
    - Lists create validity-only + structural/data entries
    - Leaves create data entries

    With ``nested_validity`` (spec Section 9.5) a nullable struct gets a
    validity-only entry, and a nullable struct or list that is a list element
    records its validity in the list's element entry (``reuse``) instead of
    replacing it.
    """
    import pyarrow as pa
    canonical = _normalize_data_type(data_type)
    nv = nested_validity
    # True only for a nullable list element: the list has just created its
    # element entry at `path` (spec Section 9.5).
    reuse = nv and nullable and path in out

    if pa.types.is_struct(canonical):
        # Struct is transparent — no entry for the struct itself (default).
        if nv and nullable and not reuse:
            out[path] = _new_validity_only()
        # Recurse into children sorted alphabetically (spec Section 3.5).
        children = [canonical.field(i) for i in range(canonical.num_fields)]
        children.sort(key=lambda f: f.name)
        for child in children:
            child_path = f"{path}{DELIMITER}{child.name}" if path else child.name
            _extract_type_entries(child.type, child.nullable, child_path, out, nv)

    elif pa.types.is_large_list(canonical) or pa.types.is_list(canonical):
        # Nullable list: validity-only entry at `path` (spec Section 3.4).
        # Default: this REPLACES an existing list-element entry.
        if nullable and not reuse:
            out[path] = _new_validity_only()

        # List level: entry at path + "/" (spec Section 3.4)
        list_path = f"{path}{DELIMITER}"
        inner_field = canonical.value_field
        inner_canonical = _normalize_data_type(inner_field.type)

        if pa.types.is_struct(inner_canonical):
            # List<Struct>: structural-only at list_path, struct children get own entries
            out[list_path] = _new_structural_only(inner_field.nullable)
            _extract_type_entries(inner_field.type, inner_field.nullable, list_path, out, nv)
        elif pa.types.is_large_list(inner_canonical) or pa.types.is_list(inner_canonical):
            # List<List>: structural-only, recurse into inner list
            out[list_path] = _new_structural_only(inner_field.nullable)
            _extract_type_entries(inner_field.type, inner_field.nullable, list_path, out, nv)
        else:
            # List<Leaf>: list-leaf entry with both structural + data
            out[list_path] = _new_list_leaf(inner_field.nullable)

    else:
        # Leaf type: data entry
        out[path] = _new_data_only(nullable)


def _extract_fields(
    field: pa.Field, parent: str, out: dict[str, tuple], nested_validity: bool = False
) -> None:
    """Extract BTreeMap entries from a schema field (record-batch path)."""
    full_name = f"{parent}{DELIMITER}{field.name}" if parent else field.name
    _extract_type_entries(field.type, field.nullable, full_name, out, nested_validity)


# ---------------------------------------------------------------------------
# Type normalization (spec Section 2.1 — type canonicalization)
# ---------------------------------------------------------------------------

def _normalize_data_type(dt: pa.DataType) -> pa.DataType:
    """Recursively normalize a DataType to its canonical large equivalent."""
    import pyarrow as pa

    if pa.types.is_dictionary(dt):
        return _normalize_data_type(dt.value_type)
    if dt == pa.utf8():
        return pa.large_utf8()
    if dt == pa.binary():
        return pa.large_binary()
    if pa.types.is_list(dt) or pa.types.is_large_list(dt):
        inner = _normalize_field(dt.value_field)
        return pa.large_list(inner)
    if pa.types.is_struct(dt):
        fields = [_normalize_field(dt.field(i)) for i in range(dt.num_fields)]
        return pa.struct(fields)
    if pa.types.is_fixed_size_list(dt):
        inner = _normalize_field(dt.value_field)
        return pa.list_(inner, dt.list_size)
    if pa.types.is_map(dt):
        key_field = _normalize_field(dt.key_field)
        item_field = _normalize_field(dt.item_field)
        return pa.map_(key_field.type, item_field.type, keys_sorted=dt.keys_sorted)
    return dt


def _normalize_field(field: pa.Field) -> pa.Field:
    """Normalize a single field: keep name and nullability, normalize the data type."""
    import pyarrow as pa
    return pa.field(field.name, _normalize_data_type(field.type), field.nullable)


# ---------------------------------------------------------------------------
# Array normalization helper
# ---------------------------------------------------------------------------

def _normalize_array(data_type, array):
    """Normalize small Arrow variants to their large canonical equivalents.

    Returns (effective_data_type, effective_array).
    """
    import pyarrow as pa

    if pa.types.is_string(data_type) and not pa.types.is_large_string(data_type):
        return pa.large_utf8(), array.cast(pa.large_utf8())
    if pa.types.is_binary(data_type) and not pa.types.is_large_binary(data_type):
        return pa.large_binary(), array.cast(pa.large_binary())
    if pa.types.is_list(data_type) and not pa.types.is_large_list(data_type):
        target = pa.large_list(data_type.value_field)
        return target, array.cast(target)
    if pa.types.is_dictionary(data_type):
        effective_type = data_type.value_type
        return _normalize_array(effective_type, array.cast(effective_type))
    return data_type, array


# ---------------------------------------------------------------------------
# Recursive traversal — populates BTreeMap entries from array data
# (spec Sections 3.1–3.5)
# ---------------------------------------------------------------------------

def _combine_null_masks(own_valid, ancestor_valid):
    """AND-combine two validity lists. Returns None if all valid."""
    if own_valid is None and ancestor_valid is None:
        return None
    if own_valid is None:
        return ancestor_valid
    if ancestor_valid is None:
        return own_valid
    return [a and b for a, b in zip(own_valid, ancestor_valid)]


def _get_validity_list(array):
    """Return a list of bools (True=valid) or None if no nulls."""
    if array.null_count == 0:
        return None
    return [array[i].is_valid for i in range(len(array))]


def _traverse_and_update(data_type, nullable, array, path, ancestor_nulls, fields, nested_validity=False):
    """Top-down recursive traversal that routes data to BTreeMap entries.

    Parameters:
        data_type: Arrow data type of the array
        nullable: whether this position is nullable
        array: the Arrow array to hash
        path: current BTreeMap key path
        ancestor_nulls: list of bools from ancestor struct nulls, or None
        fields: the BTreeMap of entries to populate
        nested_validity: spec Section 9.5 (nullable structs record their validity)
    """
    import pyarrow as pa

    effective_type, effective_array = _normalize_array(data_type, array)
    canonical = _normalize_data_type(effective_type)

    if pa.types.is_large_list(canonical):
        _traverse_list(
            effective_array, canonical.value_field, nullable, path, ancestor_nulls, fields, nested_validity
        )
    elif pa.types.is_struct(canonical):
        _traverse_struct(effective_array, nullable, path, ancestor_nulls, fields, nested_validity)
    else:
        _traverse_leaf(effective_type, effective_array, path, ancestor_nulls, fields, nested_validity)


def _traverse_list(array, value_field, nullable, path, ancestor_nulls, fields, nested_validity=False):
    """Traverse a list array, populating validity/structural/data entries (spec Section 3.4)."""
    import pyarrow as pa

    # If nullable, record list-level validity at `path`
    if nullable:
        entry = fields.get(path)
        if entry is not None:
            null_bits = entry[0]
            if null_bits is not None:
                own_valid = _get_validity_list(array)
                effective = _combine_null_masks(own_valid, ancestor_nulls)
                if effective is not None:
                    for v in effective:
                        null_bits.push(v)
                else:
                    null_bits.extend_true(len(array))

    list_path = f"{path}{DELIMITER}"

    # Determine effective null buffer for skipping null list elements
    own_valid = _get_validity_list(array)
    effective_nulls = _combine_null_masks(own_valid, ancestor_nulls)

    # For each row, write structural info and recurse into non-null elements
    offsets = array.offsets
    for i in range(len(array)):
        is_valid = effective_nulls is None or effective_nulls[i]
        if is_valid:
            start = offsets[i].as_py()
            end = offsets[i + 1].as_py()
            sub_array = array.values.slice(start, end - start)
            sub_len = len(sub_array)

            # Write list length to structural digest at list_path
            entry = fields.get(list_path)
            if entry is not None and entry[1] is not None:
                entry[1].update(struct.pack("<Q", sub_len))

            # Recurse into the sub-array using the value field's type
            _traverse_and_update(
                value_field.type,
                value_field.nullable,
                sub_array,
                list_path,
                None,  # list elements don't inherit ancestor struct nulls
                fields,
                nested_validity,
            )


def _traverse_struct(array, nullable, path, ancestor_nulls, fields, nested_validity=False):
    """Traverse a struct array — struct is transparent (spec Section 3.5).

    Struct-level nulls are AND-propagated to all descendant entries. With
    ``nested_validity`` a nullable struct also pushes that combined validity
    into its own entry's null bits (spec Section 9.5).
    """
    import pyarrow as pa

    struct_array = array
    # Combine struct's own nulls with ancestor nulls (AND propagation)
    if nullable:
        combined = _combine_null_masks(_get_validity_list(struct_array), ancestor_nulls)
    else:
        combined = ancestor_nulls

    if nested_validity and nullable:
        entry = fields.get(path)
        if entry is not None and entry[0] is not None:
            if combined is not None:
                for v in combined:
                    entry[0].push(v)
            else:
                entry[0].extend_true(len(struct_array))

    # Sort children alphabetically by field name
    children = [(i, struct_array.type.field(i)) for i in range(struct_array.type.num_fields)]
    children.sort(key=lambda x: x[1].name)

    for idx, child_field in children:
        child_array = struct_array.field(idx)
        child_path = f"{path}{DELIMITER}{child_field.name}" if path else child_field.name
        _traverse_and_update(
            child_field.type,
            child_field.nullable,
            child_array,
            child_path,
            combined,
            fields,
            nested_validity,
        )


def _traverse_leaf(data_type, array, path, ancestor_nulls, fields, nested_validity=False):
    """Traverse a leaf array — hash data into its BTreeMap entry (spec Sections 3.1–3.3)."""
    entry = fields.get(path)
    if entry is None:
        return

    null_bits, _structural, data_digest = entry
    if data_digest is None:
        return

    # Compute effective validity (own nulls AND ancestor struct nulls)
    own_valid = _get_validity_list(array)
    effective = _combine_null_masks(own_valid, ancestor_nulls)

    # Push effective validity to null_bits
    if null_bits is not None:
        if effective is not None:
            for v in effective:
                null_bits.push(v)
        else:
            null_bits.extend_true(len(array))

    # Hash leaf data, skipping null elements
    _hash_leaf_data(data_type, array, data_digest, effective, nested_validity)


def _hash_leaf_data(data_type, array, data_digest, effective_nulls, nested_validity=False):
    """Hash leaf-level data bytes into the data digest (spec Sections 3.1–3.3, 9.5.1)."""
    import pyarrow as pa

    if pa.types.is_boolean(data_type):
        if nested_validity:
            _hash_boolean_bytes(array, data_digest, effective_nulls)
        else:
            _hash_boolean_data(array, data_digest, effective_nulls)
    elif pa.types.is_large_binary(data_type):
        _hash_binary_data(array, data_digest, effective_nulls)
    elif pa.types.is_large_string(data_type):
        _hash_string_data(array, data_digest, effective_nulls)
    else:
        element_size = _element_size_for_type(data_type)
        if element_size is not None:
            _hash_fixed_size_data(array, data_digest, element_size, effective_nulls)
        else:
            raise NotImplementedError(f"Unsupported leaf type: {data_type}")


# ---------------------------------------------------------------------------
# Leaf data hashing (spec Sections 3.1–3.3)
# ---------------------------------------------------------------------------

def _element_size_for_type(dt: pa.DataType) -> int | None:
    """Return byte width for fixed-size types, or None for variable-length."""
    import pyarrow as pa

    _sizes = {
        pa.int8(): 1, pa.uint8(): 1,
        pa.int16(): 2, pa.uint16(): 2, pa.float16(): 2,
        pa.int32(): 4, pa.uint32(): 4, pa.float32(): 4, pa.date32(): 4,
        pa.int64(): 8, pa.uint64(): 8, pa.float64(): 8, pa.date64(): 8,
    }
    if dt in _sizes:
        return _sizes[dt]
    if pa.types.is_time32(dt):
        return 4
    if pa.types.is_time64(dt):
        return 8
    if pa.types.is_timestamp(dt):
        return 8  # int64 physical storage; unit/tz are schema metadata
    if pa.types.is_duration(dt):
        return 8  # int64 physical storage; unit is schema metadata
    if pa.types.is_decimal(dt):
        return dt.bit_width // 8
    if pa.types.is_fixed_size_binary(dt):
        return dt.byte_width
    return None


def _hash_fixed_size_data(arr, data_digest, element_size: int, effective_nulls) -> None:
    """Hash a fixed-width array's data bytes (spec Section 3.1)."""
    bufs = arr.buffers()
    data_buf = bufs[1]
    offset = arr.offset

    raw = data_buf.to_pybytes()
    start = offset * element_size

    if effective_nulls is None:
        # Non-nullable or all valid: feed entire contiguous buffer
        end = start + len(arr) * element_size
        data_digest.update(raw[start:end])
    else:
        # Nullable: feed only valid elements
        has_nulls = any(not v for v in effective_nulls)
        if has_nulls:
            for i in range(len(arr)):
                if effective_nulls[i]:
                    pos = start + i * element_size
                    data_digest.update(raw[pos:pos + element_size])
        else:
            end = start + len(arr) * element_size
            data_digest.update(raw[start:end])


def _hash_boolean_data(arr, data_digest, effective_nulls) -> None:
    """Hash boolean array data bits (spec Section 3.2)."""
    bv = _BitVec()
    if effective_nulls is None:
        for i in range(len(arr)):
            bv.push(arr[i].as_py())
    else:
        for i in range(len(arr)):
            if effective_nulls[i]:
                bv.push(arr[i].as_py())
    data_digest.update(bv.raw_bytes())


# Elements per chunk when expanding Booleans to one byte each (bounds temporary memory).
_BOOLEAN_CHUNK = 1 << 16


def _hash_boolean_bytes(arr, data_digest, effective_nulls) -> None:
    """Hash boolean data as one byte (0x00/0x01) per valid value (spec Section 9.5.1).

    Works in chunks of ``_BOOLEAN_CHUNK`` elements: consecutive digest updates give
    the same bytes as one update, without expanding the whole column at once.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    for start in range(0, len(arr), _BOOLEAN_CHUNK):
        chunk = arr.slice(start, _BOOLEAN_CHUNK)
        if effective_nulls is not None:
            mask = pa.array(effective_nulls[start:start + len(chunk)], pa.bool_())
            chunk = pc.filter(chunk, mask)
        as_u8 = pc.cast(chunk, pa.uint8())
        data_digest.update(
            as_u8.buffers()[1].to_pybytes()[as_u8.offset:as_u8.offset + len(as_u8)]
        )


def _hash_binary_data(arr, data_digest, effective_nulls) -> None:
    """Hash binary array data (spec Section 3.3)."""
    if effective_nulls is None:
        for i in range(len(arr)):
            val = arr[i].as_py()
            data_digest.update(struct.pack("<Q", len(val)))
            data_digest.update(val)
    else:
        for i in range(len(arr)):
            if effective_nulls[i]:
                val = arr[i].as_py()
                data_digest.update(struct.pack("<Q", len(val)))
                data_digest.update(val)


def _hash_string_data(arr, data_digest, effective_nulls) -> None:
    """Hash string array data (spec Section 3.3)."""
    if effective_nulls is None:
        for i in range(len(arr)):
            val = arr[i].as_py().encode("utf-8")
            data_digest.update(struct.pack("<Q", len(val)))
            data_digest.update(val)
    else:
        for i in range(len(arr)):
            if effective_nulls[i]:
                val = arr[i].as_py().encode("utf-8")
                data_digest.update(struct.pack("<Q", len(val)))
                data_digest.update(val)


# ---------------------------------------------------------------------------
# Finalization (spec Section 4)
# ---------------------------------------------------------------------------

def _finalize_digest(final_digest, entry: tuple) -> None:
    """Finalize a single BTreeMap entry into the final combining digest (spec Section 4)."""
    null_bits, structural, data = entry

    # 1. null_bits (if present — nullable entries only)
    if null_bits is not None:
        final_digest.update(struct.pack("<Q", len(null_bits)))
        for b in null_bits.raw_bytes():
            final_digest.update(bytes([b]))

    # 2. structural (if present — list entries only)
    if structural is not None:
        final_digest.update(structural.digest())

    # 3. data (if present — leaf and list-leaf entries only)
    if data is not None:
        final_digest.update(data.digest())


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class ArrowDigester:
    """Pure-Python equivalent of the Rust ``ArrowDigester``.

    Produces identical SHA-256 hashes with a 3-byte version prefix
    (hash format version 0.0.1, independent of the package version).

    By default, Arrow schema- and field-level metadata are excluded from
    the hash. Pass ``include_metadata=True`` to any entry point to include
    them — see the ``include_metadata`` parameter on each method.
    A schema with no metadata produces the same hash regardless of that
    flag (empty-metadata invariant).

    Three further keyword-only options, all off by default, are described in
    spec Section 9: ``ordered_columns``, ``ordered_struct_fields`` and
    ``nested_validity``. With all three off every hash is exactly as before.
    """

    def __init__(
        self,
        schema: pa.Schema,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
        nested_validity: bool = False,
    ) -> None:
        """Initialize a streaming Arrow hasher.

        Args:
            schema: The Arrow schema all record batches must conform to.
            include_metadata: When True, schema-level and per-field Arrow
                metadata are included in the hash. Default is False,
                preserving hash format 0.0.1 stability. A schema with no
                metadata produces the same hash regardless of this flag
                (empty-metadata invariant).
            ordered_columns: When True, the order of the top-level columns is
                part of the hash (spec Section 9.3).
            ordered_struct_fields: When True, the order of struct fields, at
                every nesting depth, is part of the hash (spec Section 9.3).
            nested_validity: When True, every nullable struct, and every
                nullable list or struct that is a list element, records its
                own validity bits (spec Section 9.5).

        With any of these three options on, the hash never equals the hash
        taken with a different set of options (in particular with all of them
        off), and schemas in which two nodes share a path (duplicate names, or
        a name containing ``/`` that aliases another node) are rejected with
        ``ValueError`` (spec Section 9.6).

        With any option on, every batch passed to :meth:`update` must have the
        same canonical schema as ``schema`` under those options, otherwise
        ``ValueError`` is raised (spec Section 9.7). For example, with
        ``ordered_columns`` a batch with the columns in a different order is
        rejected.

        Raises:
            ValueError: With any option on, if ``schema`` is rejected (spec 9.6).
        """
        if _FORMAT_2:
            _reject_nested_validity(nested_validity)
            from starfix import _format2

            self._format2 = _format2.Digester(
                schema,
                include_metadata=include_metadata,
                ordered_columns=ordered_columns,
                ordered_struct_fields=ordered_struct_fields,
            )
            return
        self._format2 = None
        flags = _flags(ordered_columns, ordered_struct_fields, nested_validity)
        if flags:
            _check_schema_node_paths(schema)
        self._schema = schema
        self._include_metadata = include_metadata  # captured in _schema_digest; reused by update()
        self._flags = flags
        self._schema_digest = _hash_schema(schema, include_metadata=include_metadata, flags=flags)
        # Spec Section 9.7: with any option on, every batch must match this key.
        self._schema_key = (
            _schema_key(schema, include_metadata, flags) if flags or _FIX_DEFAULT_BATCH_CHECK else None
        )
        # BTreeMap<path, (BitVec|None, sha256|None, sha256|None)> — sorted by key
        self._fields: dict[str, tuple] = {}
        nv = _lossless(flags)
        for i in range(len(schema)):
            _extract_fields(schema.field(i), "", self._fields, nv)
        # Ensure sorted order (Python 3.7+ dicts are insertion-ordered)
        self._fields = dict(sorted(self._fields.items()))

    def update(self, record_batch: pa.RecordBatch) -> None:
        """Feed a RecordBatch into the running digest (spec Sections 3–5).

        Raises:
            ValueError: With any option on, if the batch's schema is rejected
                (spec 9.6) or does not match the digester's schema under its
                options (spec 9.7).
        """
        if self._format2 is not None:
            self._format2.update(record_batch)
            return
        schema = record_batch.schema
        if self._flags:
            _check_schema_node_paths(schema)
        if self._schema_key is not None:
            if _schema_key(schema, self._include_metadata, self._flags) != self._schema_key:
                raise ValueError(_SCHEMA_MISMATCH)
        nv = _lossless(self._flags)
        # Traverse each top-level field using the recursive traversal
        for i in range(len(schema)):
            field = schema.field(i)
            col = record_batch.column(i)
            _traverse_and_update(
                field.type,
                field.nullable,
                col,
                field.name,
                None,  # no ancestor struct nulls at top level
                self._fields,
                nv,
            )

    def finalize(self) -> bytes:
        """Consume the digester and return the versioned hash (spec Section 5)."""
        if self._format2 is not None:
            return self._format2.finalize()
        final_digest = hashlib.sha256()
        final_digest.update(self._schema_digest)
        for _path, entry in sorted(self._fields.items()):
            _finalize_digest(final_digest, entry)
        return VERSION_BYTES + final_digest.digest()

    # -- Convenience class methods ------------------------------------------

    @staticmethod
    def hash_schema(
        schema: pa.Schema,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
        nested_validity: bool = False,
    ) -> bytes:
        """Hash an Arrow schema.

        Args:
            schema: The schema to hash.
            include_metadata: When True, schema-level and per-field Arrow
                metadata are included in the hash. Default is False,
                preserving hash format 0.0.1 stability. A schema with no
                metadata produces the same hash regardless of this flag
                (empty-metadata invariant).
            ordered_columns: When True, the order of the top-level columns is
                part of the hash (spec Section 9.3).
            ordered_struct_fields: When True, the order of struct fields, at
                every nesting depth, is part of the hash (spec Section 9.3).
            nested_validity: When True, every nullable struct, and every
                nullable list or struct that is a list element, records its
                own validity bits (spec Section 9.5).

        With any of these three options on, the hash never equals the hash
        taken with a different set of options (in particular with all of them
        off), and schemas in which two nodes share a path (duplicate names, or
        a name containing ``/`` that aliases another node) are rejected with
        ``ValueError`` (spec Section 9.6).
        ``nested_validity`` does not change the schema JSON, but like every
        option it is part of the hash.

        Raises:
            ValueError: With any option on, if ``schema`` is rejected (spec 9.6).
        """
        if _FORMAT_2:
            _reject_nested_validity(nested_validity)
            from starfix import _format2

            return _format2.hash_schema(
                schema,
                include_metadata=include_metadata,
                ordered_columns=ordered_columns,
                ordered_struct_fields=ordered_struct_fields,
            )
        flags = _flags(ordered_columns, ordered_struct_fields, nested_validity)
        if flags:
            _check_schema_node_paths(schema)
        return VERSION_BYTES + _hash_schema(schema, include_metadata=include_metadata, flags=flags)

    @staticmethod
    def hash_record_batch(
        record_batch: pa.RecordBatch,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
        nested_validity: bool = False,
    ) -> bytes:
        """Hash an Arrow record batch.

        Args:
            record_batch: The record batch to hash.
            include_metadata: When True, schema-level and per-field Arrow
                metadata are included in the hash. Default is False,
                preserving hash format 0.0.1 stability. A schema with no
                metadata produces the same hash regardless of this flag
                (empty-metadata invariant).
            ordered_columns: When True, the order of the top-level columns is
                part of the hash (spec Section 9.3).
            ordered_struct_fields: When True, the order of struct fields, at
                every nesting depth, is part of the hash (spec Section 9.3).
            nested_validity: When True, every nullable struct, and every
                nullable list or struct that is a list element, records its
                own validity bits (spec Section 9.5).

        With any of these three options on, the hash never equals the hash
        taken with a different set of options (in particular with all of them
        off), and schemas in which two nodes share a path (duplicate names, or
        a name containing ``/`` that aliases another node) are rejected with
        ``ValueError`` (spec Section 9.6).
        Raises:
            ValueError: With any option on, if the schema is rejected (spec 9.6).
        """
        d = ArrowDigester(
            record_batch.schema,
            include_metadata=include_metadata,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
            nested_validity=nested_validity,
        )
        d.update(record_batch)
        return d.finalize()

    @staticmethod
    def hash_table(
        table: pa.Table,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
        nested_validity: bool = False,
    ) -> bytes:
        """Hash a full Arrow table (iterates over all batches).

        Equivalent to ``ArrowDigester(table.schema, ...)``, :meth:`update` for each
        batch of ``table.to_batches()``, then :meth:`finalize`.

        Args:
            table: The table to hash.
            include_metadata: When True, schema-level and per-field Arrow
                metadata are included in the hash. Default is False,
                preserving hash format 0.0.1 stability. A schema with no
                metadata produces the same hash regardless of this flag
                (empty-metadata invariant).
            ordered_columns: When True, the order of the top-level columns is
                part of the hash (spec Section 9.3).
            ordered_struct_fields: When True, the order of struct fields, at
                every nesting depth, is part of the hash (spec Section 9.3).
            nested_validity: When True, every nullable struct, and every
                nullable list or struct that is a list element, records its
                own validity bits (spec Section 9.5).

        With any of these three options on, the hash never equals the hash
        taken with a different set of options (in particular with all of them
        off), and schemas in which two nodes share a path (duplicate names, or
        a name containing ``/`` that aliases another node) are rejected with
        ``ValueError`` (spec Section 9.6).
        Raises:
            ValueError: With any option on, if the schema is rejected (spec 9.6).
        """
        d = ArrowDigester(
            table.schema,
            include_metadata=include_metadata,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
            nested_validity=nested_validity,
        )
        for batch in table.to_batches():
            d.update(batch)
        return d.finalize()

    @staticmethod
    def hash_array(
        array: pa.Array,
        *,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
        nested_validity: bool = False,
    ) -> bytes:
        """Hash a single array (spec Section 6).

        Uses the same recursive BTreeMap decomposition as the record-batch path.

        Args:
            array: The array to hash.
            ordered_columns: When True, the order of the top-level columns is
                part of the hash (spec Section 9.3).
            ordered_struct_fields: When True, the order of struct fields, at
                every nesting depth, is part of the hash (spec Section 9.3).
            nested_validity: When True, every nullable struct, and every
                nullable list or struct that is a list element, records its
                own validity bits (spec Section 9.5).

        With any of these three options on, the hash never equals the hash
        taken with a different set of options (in particular with all of them
        off), and schemas in which two nodes share a path (duplicate names, or
        a name containing ``/`` that aliases another node) are rejected with
        ``ValueError`` (spec Section 9.6).
        ``ordered_columns`` does not change the type JSON of an array, but like
        every option it is part of the hash (spec Section 9.4).

        Raises:
            ValueError: With any option on, if the array's type is rejected
                (spec 9.6).

        Note:
            ``include_metadata`` is intentionally absent here — standalone arrays
            carry no schema or field metadata. This matches the Rust ``starfix`` API.
        """
        import pyarrow as pa

        if _FORMAT_2:
            _reject_nested_validity(nested_validity)
            from starfix import _format2

            return _format2.hash_array(
                array, ordered_columns=ordered_columns, ordered_struct_fields=ordered_struct_fields
            )
        flags = _flags(ordered_columns, ordered_struct_fields, nested_validity)
        nv = _lossless(flags)

        # Resolve dictionary arrays to their plain value type
        effective_type = array.type
        effective_array = array
        if pa.types.is_dictionary(effective_type):
            effective_type = effective_type.value_type
            effective_array = array.cast(effective_type)

        if flags:
            _check_type_node_paths(effective_type)

        # Normalize to canonical large types
        normalized_type = _normalize_data_type(effective_type)

        final_digest = hashlib.sha256()
        if flags:
            # Spec Section 9.2: the options tag is the first input.
            final_digest.update(_options_tag(flags))

        # Step 1: Type metadata (canonical JSON string)
        final_digest.update(_type_json(normalized_type, flags).encode())

        # Determine nullability: arrays with null_count > 0 are nullable
        nullable = effective_array.null_count > 0

        # Step 2: Build BTreeMap entries from the type tree (same as record-batch)
        fields: dict[str, tuple] = {}
        _extract_type_entries(effective_type, nullable, "", fields, nv)
        fields = dict(sorted(fields.items()))

        # Step 3: Traverse and populate entries
        _traverse_and_update(
            effective_type,
            nullable,
            effective_array,
            "",
            None,
            fields,
            nv,
        )

        # Step 4: Finalize all entries into the digest
        for _path, entry in sorted(fields.items()):
            _finalize_digest(final_digest, entry)

        return VERSION_BYTES + final_digest.digest()

