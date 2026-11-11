"""Hash format 2 of the starfix Arrow logical hasher (ITL-898).

Implements ``nauticalab/starfix docs/byte-layout-spec.md`` (format 2) from the
specification alone. Section numbers below refer to that document.
"""

from __future__ import annotations

import hashlib
import json
import struct
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    from collections.abc import Iterable

FORMAT = 2
VERSION_BYTES = FORMAT.to_bytes(3, "big")  # Section 1: 24-bit big-endian format number
_HEADER_PREFIX = b"\x00starfix" + VERSION_BYTES

ORDERED_COLUMNS = 0x01
ORDERED_STRUCT_FIELDS = 0x02

# Section 1: entry-point kind, one ASCII byte of the header.
KIND_SCHEMA = b"S"  # schema digest (hash_schema, and the schema digest inside record hashes)
KIND_RECORDS = b"R"  # record batch / stream / table final digest
KIND_ARRAY = b"A"  # hash_array

_SCHEMA_MISMATCH = "Record batch schema does not match ArrowDigester schema"
_U64 = struct.Struct("<Q")
_CHUNK = 1 << 16  # Booleans per chunk when expanding them to one byte each
_BYTE_BUDGET = 1 << 20  # bytes per digest update when serializing variable-length values


def flags_of(ordered_columns: bool, ordered_struct_fields: bool) -> int:
    return (ORDERED_COLUMNS if ordered_columns else 0) | (ORDERED_STRUCT_FIELDS if ordered_struct_fields else 0)


def header(kind: bytes, flags: int) -> bytes:
    """Section 1: 0x00 || "starfix" || version || kind || flags (13 bytes)."""
    return _HEADER_PREFIX + kind + bytes([flags])


def _json(value: Any) -> str:
    """Section 2.1 JSON text rules (serde_json compact form; keys sorted by code point)."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


# ---------------------------------------------------------------------------
# Types: unwrapping, rejection, canonical JSON (Sections 2.1, 7)
# ---------------------------------------------------------------------------

def _is_extension(t: pa.DataType) -> bool:
    return isinstance(t, pa.BaseExtensionType)


def _logical(t: pa.DataType) -> pa.DataType:
    """Extension -> storage type, dictionary -> value type (repeatedly)."""
    while True:
        if _is_extension(t):
            t = t.storage_type
        elif pa.types.is_dictionary(t):
            t = t.value_type
        else:
            return t


def _is_rejected_type(t: pa.DataType) -> bool:
    checks = ("is_list_view", "is_large_list_view", "is_union", "is_run_end_encoded")
    return any(getattr(pa.types, name, lambda _t: False)(t) for name in checks)


def _is_list(t: pa.DataType) -> bool:
    return pa.types.is_list(t) or pa.types.is_large_list(t)


_UNITS = {"s": "Second", "ms": "Millisecond", "us": "Microsecond", "ns": "Nanosecond"}
_INTERVALS = {"month_interval": "YearMonth", "day_time_interval": "DayTime", "month_day_nano_interval": "MonthDayNano"}
_SIMPLE = {
    "null": "Null", "bool": "Boolean",
    "int8": "Int8", "int16": "Int16", "int32": "Int32", "int64": "Int64",
    "uint8": "UInt8", "uint16": "UInt16", "uint32": "UInt32", "uint64": "UInt64",
    "halffloat": "Float16", "float": "Float32", "double": "Float64",
    "date32[day]": "Date32", "date64[ms]": "Date64",
    "string": "LargeUtf8", "large_string": "LargeUtf8", "binary": "LargeBinary", "large_binary": "LargeBinary",
    "string_view": "Utf8View", "binary_view": "BinaryView",
}


def type_value(t: pa.DataType, flags: int) -> Any:
    """The canonical type JSON value of `t` (Section 2.1)."""
    t = _logical(t)
    if _is_rejected_type(t):
        raise ValueError(f"Unsupported Arrow type for hashing: {t}")
    if pa.types.is_struct(t):
        children = [t.field(i) for i in range(t.num_fields)]
        if not flags & ORDERED_STRUCT_FIELDS:
            children.sort(key=lambda f: f.name)
        return {"Struct": [_child(c, flags) for c in children]}
    if _is_list(t):
        return {"LargeList": _element(t.value_field, flags)}
    if pa.types.is_fixed_size_list(t):
        return {"FixedSizeList": [_element(t.value_field, flags), t.list_size]}
    if pa.types.is_map(t):
        return {"Map": {
            "key": _element(t.key_field, flags),
            "keys_sorted": t.keys_sorted,
            "value": _element(t.item_field, flags),
        }}
    name = str(t)
    if name in _SIMPLE:
        return _SIMPLE[name]
    if name in _INTERVALS:
        return {"Interval": _INTERVALS[name]}
    if pa.types.is_time32(t):
        return {"Time32": _UNITS[t.unit]}
    if pa.types.is_time64(t):
        return {"Time64": _UNITS[t.unit]}
    if pa.types.is_timestamp(t):
        return {"Timestamp": [_UNITS[t.unit], t.tz]}
    if pa.types.is_duration(t):
        return {"Duration": _UNITS[t.unit]}
    if pa.types.is_decimal(t):
        return {f"Decimal{t.bit_width}": [t.precision, t.scale]}
    if pa.types.is_fixed_size_binary(t):
        return {"FixedSizeBinary": t.byte_width}
    raise NotImplementedError(f"Unsupported Arrow type for hashing: {t}")


def _child(field: pa.Field, flags: int) -> dict:
    return {"data_type": type_value(field.type, flags), "name": field.name, "nullable": field.nullable}


def _element(field: pa.Field, flags: int) -> dict:
    return {"data_type": type_value(field.type, flags), "nullable": field.nullable}


def type_json(t: pa.DataType, flags: int) -> str:
    return _json(type_value(t, flags))


# ---------------------------------------------------------------------------
# Node paths (Sections 3.1, 7) and Phase 1 / Phase 2 (Sections 2.2, 2.3)
# ---------------------------------------------------------------------------

def _children(field_type: pa.DataType, path: str) -> list[tuple[str, pa.Field]]:
    """The child nodes of a node of this type, as (node path, field)."""
    t = _logical(field_type)
    if _is_rejected_type(t):
        raise ValueError(f"Unsupported Arrow type for hashing: {t}")
    if pa.types.is_struct(t):
        # Section 3.1: always p + "/" + name (also when p or name is empty).
        return [(f"{path}/{t.field(i).name}", t.field(i)) for i in range(t.num_fields)]
    if _is_list(t) or pa.types.is_fixed_size_list(t):
        return [(path + "/", t.value_field)]
    if pa.types.is_map(t):
        # Map nodes are p, p/, p//key, p//value; the entries field contributes nothing.
        return [(path + "//key", t.key_field), (path + "//value", t.item_field)]
    return []


def _walk(root: Iterable[tuple[str, pa.Field]]) -> list[tuple[str, pa.Field]]:
    """Every node below and including `root`, rejecting duplicate node paths (Section 7)."""
    seen: set[str] = set()
    out: list[tuple[str, pa.Field]] = []
    stack = list(reversed(list(root)))
    while stack:
        path, field = stack.pop()
        claimed = [path]
        t = _logical(field.type)
        if pa.types.is_map(t):
            claimed.append(path + "/")  # the map's element node
        for p in claimed:
            if p in seen:
                raise ValueError(
                    f"Ambiguous node path {p!r}: two schema nodes share one path "
                    "(duplicate names, or a name containing '/' that aliases another node)"
                )
            seen.add(p)
        out.append((path, field))
        stack.extend(reversed(_children(field.type, path)))
    return out


def schema_nodes(schema: pa.Schema) -> list[tuple[str, pa.Field]]:
    return _walk((schema.field(i).name, schema.field(i)) for i in range(len(schema)))


def phase1_json(schema: pa.Schema, flags: int) -> str:
    """Section 2.2."""
    fields = [schema.field(i) for i in range(len(schema))]
    if flags & ORDERED_COLUMNS:
        return _json([_child(f, flags) for f in fields])
    return _json({f.name: {"data_type": type_value(f.type, flags), "nullable": f.nullable} for f in fields})


def _decode(mapping) -> dict[str, str]:
    if not mapping:
        return {}
    return {
        (k.decode("utf-8") if isinstance(k, bytes) else k): (v.decode("utf-8") if isinstance(v, bytes) else v)
        for k, v in mapping.items()
    }


def _extension_metadata(t: pa.DataType) -> dict[str, str]:
    """ARROW:extension:* keys of a registered extension type (pyarrow hides them).

    Python-defined ExtensionType subclasses serialize their stored bytes. pyarrow's
    natively registered canonical types have no __arrow_ext_serialize__; their metadata
    is what pyarrow exports for the type, its canonical serialization, and the spec
    (Section 2.3) defines their hash only for that canonical stored form.
    """
    if not _is_extension(t):
        return {}
    if hasattr(t, "__arrow_ext_serialize__"):
        serialized = t.__arrow_ext_serialize__()
    else:
        serialized = _exported_extension_metadata(t)
    return {"ARROW:extension:name": t.extension_name, "ARROW:extension:metadata": serialized.decode("utf-8")}


def _exported_metadata_pairs(obj) -> list[tuple[bytes, bytes]]:
    """Key/value pairs of a Field's, Schema's or DataType's metadata as Arrow exports them.

    Read through the Arrow C data interface (ArrowSchema.metadata, a fixed, documented ABI:
    int32 count, then per pair int32 length + key bytes, int32 length + value bytes). Unlike
    pyarrow's ``metadata`` dict this keeps duplicate keys, and for a registered extension
    type it carries the ARROW:extension:* keys pyarrow writes for the type.
    """
    import ctypes

    class _ArrowSchema(ctypes.Structure):
        pass

    _ArrowSchema._fields_ = [
        ("format", ctypes.c_char_p),
        ("name", ctypes.c_char_p),
        ("metadata", ctypes.c_void_p),
        ("flags", ctypes.c_int64),
        ("n_children", ctypes.c_int64),
        ("children", ctypes.c_void_p),
        ("dictionary", ctypes.c_void_p),
        ("release", ctypes.CFUNCTYPE(None, ctypes.c_void_p)),
        ("private_data", ctypes.c_void_p),
    ]
    c_schema = _ArrowSchema()
    obj._export_to_c(ctypes.addressof(c_schema))
    try:
        pairs: list[tuple[bytes, bytes]] = []
        if c_schema.metadata:
            pos = c_schema.metadata

            def read_int32() -> int:
                nonlocal pos
                value = ctypes.c_int32.from_address(pos).value
                pos += 4
                return value

            def read_bytes(n: int) -> bytes:
                nonlocal pos
                value = ctypes.string_at(pos, n)
                pos += n
                return value

            for _ in range(read_int32()):
                key = read_bytes(read_int32())
                pairs.append((key, read_bytes(read_int32())))
        return pairs
    finally:
        if c_schema.release:
            c_schema.release(ctypes.addressof(c_schema))


def _exported_extension_metadata(t: pa.DataType) -> bytes:
    """The serialized metadata pyarrow writes for a (natively registered) extension type."""
    return dict(_exported_metadata_pairs(pa.field("x", t))).get(b"ARROW:extension:metadata", b"")


def _unique_metadata(pairs: list[tuple[bytes, bytes]], where: str) -> dict[str, str]:
    """Decode metadata pairs; Section 7: a duplicate key is ambiguous and rejected."""
    out: dict[str, str] = {}
    for key, value in pairs:
        k = key.decode("utf-8")
        if k in out:
            raise ValueError(f"Duplicate metadata key {k!r} on {where}: ambiguous, rejected with include_metadata")
        out[k] = value.decode("utf-8")
    return out


def _field_metadata(field: pa.Field, path: str) -> dict[str, str]:
    """The field's metadata as stored (Section 2.3), duplicates rejected (Section 7)."""
    explicit = _decode(field.metadata)  # pyarrow's dict view (duplicates collapsed)
    t = field.type
    while pa.types.is_dictionary(t):  # a dictionary of an extension type keeps its keys
        t = t.value_type
    derived = _extension_metadata(t)
    for key, value in derived.items():
        if key in explicit and explicit[key] != value:
            raise ValueError(
                f"Field {path!r} carries {key}={explicit[key]!r}, conflicting with its extension type "
                f"({value!r}): ambiguous, rejected with include_metadata"
            )
    meta = _unique_metadata(_exported_metadata_pairs(field), f"field {path!r}")
    for key, value in derived.items():
        meta.setdefault(key, value)
    # Section 2.3: an absent ARROW:extension:metadata is hashed as "" (absent == empty).
    if "ARROW:extension:name" in meta:
        meta.setdefault("ARROW:extension:metadata", "")
    return meta


def phase2_json(schema: pa.Schema) -> str | None:
    """Section 2.3: metadata keyed by node path; None when there is none."""
    fields = {}
    for path, field in schema_nodes(schema):
        meta = _field_metadata(field, path)
        if meta:
            fields[path] = meta
    doc: dict[str, Any] = {}
    if fields:
        doc["fields"] = fields
    schema_meta = _unique_metadata(_exported_metadata_pairs(schema), "the schema")
    if schema_meta:
        doc["schema"] = schema_meta
    return _json(doc) if doc else None


def schema_key(schema: pa.Schema, include_metadata: bool, flags: int) -> tuple[str, str | None]:
    return phase1_json(schema, flags), (phase2_json(schema) if include_metadata else None)


def schema_digest(schema: pa.Schema, include_metadata: bool, flags: int) -> bytes:
    """Section 2.5."""
    schema_nodes(schema)  # Section 7 rejections
    phase1, phase2 = schema_key(schema, include_metadata, flags)
    h = hashlib.sha256(header(KIND_SCHEMA, flags))
    h.update(phase1.encode())
    if phase2 is not None:
        h.update(phase2.encode())
    return h.digest()


# ---------------------------------------------------------------------------
# Entries (Section 3.2)
# ---------------------------------------------------------------------------

class _Bits:
    """Validity bits: u8 words, Lsb0, appended in bulk."""

    __slots__ = ("_bytes", "_len")

    def __init__(self) -> None:
        self._bytes = bytearray()
        self._len = 0

    def append_bitmap(self, bitmap: pa.Buffer, bit_offset: int, n: int) -> None:
        if n:
            raw = memoryview(bitmap)[bit_offset >> 3:(bit_offset + n + 7) >> 3]
            self._append_int((int.from_bytes(raw, "little") >> (bit_offset & 7)) & ((1 << n) - 1), n)

    def append_true(self, n: int) -> None:
        self._append_int((1 << n) - 1, n)

    def _append_int(self, value: int, n: int) -> None:
        if n == 0:
            return
        shift = self._len & 7
        if shift:
            # Replace the partial last byte and append in one slice assignment, so the
            # buffer is never shorter than before (snapshot/restore relies on it).
            value = (value << shift) | self._bytes[-1]
            self._bytes[-1:] = value.to_bytes((n + shift + 7) >> 3, "little")
        else:
            self._bytes += value.to_bytes((n + 7) >> 3, "little")
        self._len += n

    def feed(self, digest) -> None:
        digest.update(_U64.pack(self._len))
        digest.update(memoryview(self._bytes))

    def snapshot(self) -> tuple[int, int, int | None]:
        """O(1) state: appends only add bytes or change the last (partial) byte."""
        return self._len, len(self._bytes), (self._bytes[-1] if self._bytes else None)

    def restore(self, state: tuple[int, int, int | None]) -> None:
        bit_len, byte_len, last = state
        del self._bytes[byte_len:]  # appends only grow the buffer or rewrite its last byte
        if last is not None:
            self._bytes[byte_len - 1] = last
        self._len = bit_len


class _Entry:
    __slots__ = ("validity", "lengths", "values", "kind")

    def __init__(self, field: pa.Field) -> None:
        t = _logical(field.type)
        self.kind = "struct" if pa.types.is_struct(t) else "list" if _is_list(t) else "leaf"
        self.validity = _Bits() if field.nullable else None
        self.lengths = hashlib.sha256() if self.kind == "list" else None
        self.values = hashlib.sha256() if self.kind == "leaf" else None

    def snapshot(self) -> tuple:
        return (
            self.validity.snapshot() if self.validity is not None else None,
            self.lengths.copy() if self.lengths is not None else None,
            self.values.copy() if self.values is not None else None,
        )

    def restore(self, state: tuple) -> None:
        validity, self.lengths, self.values = state
        if validity is not None:
            self.validity.restore(validity)

    def feed(self, digest) -> None:
        """Section 3.2: [validity][lengths][values], fed without copying the bits."""
        if self.validity is not None:
            self.validity.feed(digest)
        if self.lengths is not None:
            digest.update(self.lengths.digest())
        if self.values is not None:
            digest.update(self.values.digest())


def _resolve(arr: pa.Array) -> pa.Array:
    """Extension arrays -> storage, repeatedly. Dictionaries are hashed as their values:

    - a dictionary of leaf values stays encoded (its values are read through the indices
      from the dictionary buffers, so the logical payload is never materialized); nested
      dictionaries and extension values are resolved inside its dictionary only;
    - a dictionary of struct or list values is decoded (its children are traversed).
    """
    while True:
        if isinstance(arr, pa.ExtensionArray):
            arr = arr.storage
        elif pa.types.is_dictionary(arr.type):
            values = _resolve(arr.dictionary)
            if pa.types.is_dictionary(values.type):  # resolved values are plain or decoded
                values = values.dictionary_decode()
            t = values.type
            if pa.types.is_struct(t) or _is_list(t) or pa.types.is_fixed_size_list(t) or pa.types.is_map(t):
                arr = values.take(arr.indices)
            else:
                return arr if values is arr.dictionary else pa.DictionaryArray.from_arrays(arr.indices, values)
        else:
            return arr


def _logical_validity(arr: pa.Array) -> pa.Array | None:
    """Per-element validity (None: all valid). A dictionary element is valid iff its index
    is valid and the dictionary value it points to is valid."""
    if pa.types.is_dictionary(arr.type):
        valid = pc.fill_null(pc.take(arr.dictionary.is_valid(), arr.indices), False)
        return None if pc.all(valid).as_py() is not False else valid
    return arr.is_valid() if arr.null_count else None


def _and(a: pa.Array | None, b: pa.Array | None) -> pa.Array | None:
    if a is None:
        return b
    if b is None:
        return a
    return pc.and_(a, b)


_FIXED_WIDTH_CHECKS = (
    pa.types.is_integer, pa.types.is_floating, pa.types.is_date, pa.types.is_time,
    pa.types.is_timestamp, pa.types.is_duration, pa.types.is_decimal, pa.types.is_fixed_size_binary,
)


def _value_width(t: pa.DataType) -> int | None:
    if any(check(t) for check in _FIXED_WIDTH_CHECKS):
        return t.byte_width if pa.types.is_fixed_size_binary(t) else t.bit_width // 8
    return None


def _is_var_length(t: pa.DataType) -> bool:
    return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_binary(t) or pa.types.is_large_binary(t)


def _var_length_buffers(arr: pa.Array) -> tuple[memoryview, memoryview]:
    """The whole offsets buffer (as integers) and data buffer of a string/binary array."""
    large = pa.types.is_large_string(arr.type) or pa.types.is_large_binary(arr.type)
    size = 8 if large else 4
    bufs = arr.buffers()
    raw = memoryview(bufs[1])
    offsets = raw[: len(raw) // size * size].cast("q" if large else "i")
    data = memoryview(bufs[2]) if bufs[2] is not None else memoryview(b"")
    return offsets, data


class _VarLengthFeeder:
    """Feeds `u64 length || bytes` values to a digest in updates of about _BYTE_BUDGET bytes;
    a value of at least that size is fed straight from its buffer (no copy)."""

    __slots__ = ("digest", "parts", "pending")

    def __init__(self, digest) -> None:
        self.digest, self.parts, self.pending = digest, [], 0

    def flush(self) -> None:
        if self.parts:
            self.digest.update(b"".join(self.parts))
            self.parts, self.pending = [], 0


def _feed_var_length(feeder: "_VarLengthFeeder", data: memoryview, pairs) -> None:
    pack = _U64.pack
    for a, b in pairs:
        size = b - a
        if size >= _BYTE_BUDGET:
            feeder.parts.append(pack(size))
            feeder.flush()
            feeder.digest.update(data[a:b])
            continue
        feeder.parts.append(pack(size))
        feeder.parts.append(data[a:b])
        feeder.pending += 8 + size
        if feeder.pending >= _BYTE_BUDGET:
            feeder.flush()


def _hash_dictionary_values(arr: pa.DictionaryArray, bits: pa.Array | None, digest) -> None:
    """Leaf values of a dictionary array, read through its indices (never decoded whole)."""
    values, indices, n = arr.dictionary, arr.indices, len(arr)
    t = values.type
    if _is_var_length(t):
        offsets, data = _var_length_buffers(values)
        base = values.offset
        feeder = _VarLengthFeeder(digest)
        for start in range(0, n, _CHUNK):
            chunk = indices.slice(start, _CHUNK)
            if bits is not None:
                chunk = chunk.filter(bits.slice(start, _CHUNK))
            ids = chunk.to_pylist()
            _feed_var_length(feeder, data, ((offsets[base + j], offsets[base + j + 1]) for j in ids))
        feeder.flush()
        return
    width = _value_width(t)
    if width == 0:
        return
    rows = _CHUNK if width is None else max(1, _BYTE_BUDGET // width)
    for start in range(0, n, rows):
        chunk = indices.slice(start, rows)
        if bits is not None:
            chunk = chunk.filter(bits.slice(start, rows))
        if len(chunk):
            _hash_values(values.take(chunk), None, digest)


def _hash_values(arr: pa.Array, bits: pa.Array | None, digest) -> None:
    """Section 3.2 leaf value bytes of the elements of `arr` whose bit in `bits` is 1.

    Works in bounded chunks: the valid values are never materialized as a whole.
    """
    if pa.types.is_dictionary(arr.type):
        _hash_dictionary_values(arr, bits, digest)
        return
    t = arr.type
    n = len(arr)
    if pa.types.is_boolean(t):
        for start in range(0, n, _CHUNK):
            chunk = arr.slice(start, _CHUNK)
            if bits is not None:
                chunk = chunk.filter(bits.slice(start, _CHUNK))
            chunk = pc.cast(chunk, pa.uint8())
            digest.update(memoryview(chunk.buffers()[1])[chunk.offset:chunk.offset + len(chunk)])
        return
    width = _value_width(t)
    if width == 0:
        return  # FixedSizeBinary(0): no value bytes (validity is recorded by _visit)
    if width is not None:
        if bits is None:
            if n:
                digest.update(memoryview(arr.buffers()[1])[arr.offset * width:(arr.offset + n) * width])
            return
        rows = max(1, _BYTE_BUDGET // width)
        for start in range(0, n, rows):
            chunk = arr.slice(start, rows).filter(bits.slice(start, rows))
            if len(chunk):
                raw = memoryview(chunk.buffers()[1])
                digest.update(raw[chunk.offset * width:(chunk.offset + len(chunk)) * width])
        return
    if _is_var_length(t):
        if n == 0:
            return
        offsets, data = _var_length_buffers(arr)
        feeder = _VarLengthFeeder(digest)
        # Nulls are skipped while reading the original buffers (no copy of the payload).
        for chunk_start in range(0, n, _CHUNK):
            chunk_end = min(chunk_start + _CHUNK, n)
            offs = offsets[arr.offset + chunk_start:arr.offset + chunk_end + 1].tolist()
            pairs = zip(offs, offs[1:], strict=False)  # consecutive offsets
            if bits is not None:
                flags = bits.slice(chunk_start, chunk_end - chunk_start).to_pylist()
                pairs = (pair for pair, ok in zip(pairs, flags, strict=True) if ok)
            _feed_var_length(feeder, data, pairs)
        feeder.flush()
        return
    raise ValueError(f"Hashing data of type {t} is not supported")  # checked earlier (Section 7)


def _visit(
    field: pa.Field,
    arr: pa.Array,
    path: str,
    enclosing: pa.Array | None,
    entries: dict,
    select: pa.Array | None = None,
) -> None:
    """Feed the elements of the node `field` at `path` into its entry (Section 3.2).

    `arr` holds the node's candidate elements; `select` (None: all) marks which of them are
    elements at all: below a list, the items of valid list values (items of null or masked
    list values are not visited). `enclosing` is the AND of the enclosing struct validities
    at this list level (None: all valid). Value payloads (string/binary bytes, list
    children) are not copied: list children are zero-copy slices of the original values with
    a selection mask. Per-element metadata is still allocated: validity bitmaps, list lengths
    cast to u64, and list_parent_indices (8 bytes per list item).
    """
    arr = _resolve(arr)
    entry = entries[path]
    own = _logical_validity(arr)
    if own is not None and not field.nullable:
        # Section 7 item 4: a null in a non-nullable node not masked by an enclosing struct
        # null (elements outside `select` are not elements and are ignored).
        ok = own if enclosing is None else pc.or_(own, pc.invert(enclosing))
        if select is not None:
            ok = pc.or_(ok, pc.invert(select))
        if not pc.all(ok).as_py():
            raise ValueError(f"Non-nullable field {path!r} contains nulls")
    bits = _and(own, enclosing)

    if entry.validity is not None:
        if bits is None:
            entry.validity.append_true(len(arr) if select is None else (pc.sum(select).as_py() or 0))
        else:
            written = bits if select is None else bits.filter(select)
            entry.validity.append_bitmap(written.buffers()[1], written.offset, len(written))

    if entry.kind == "struct":
        t = arr.type
        for i in range(t.num_fields):
            child = t.field(i)
            _visit(child, arr.field(i), f"{path}/{child.name}", bits, entries, select)
        return

    present = _and(bits, select)  # elements that are visited and valid (None: all)
    if entry.kind == "list":
        if len(arr) == 0:
            # An empty list array may legally have no offsets buffer: touch neither the
            # offsets nor the lengths; the element node still gets visited (no elements).
            _visit(arr.type.value_field, arr.values.slice(0, 0), path + "/", None, entries)
            return
        lengths = arr.value_lengths()
        if present is not None:
            lengths = lengths.filter(present)
        if len(lengths):
            lengths = pc.cast(lengths, pa.uint64())
            entry.lengths.update(
                memoryview(lengths.buffers()[1])[lengths.offset * 8:(lengths.offset + len(lengths)) * 8]
            )
        # The items of all list values, as a zero-copy slice of the child, with the items of
        # null (or not visited) list values deselected. List boundaries reset `enclosing`.
        offsets = arr.offsets
        first, last = offsets[0].as_py(), offsets[len(arr)].as_py()
        items = arr.values.slice(first, last - first)
        item_select = None if present is None else pc.take(present, pc.list_parent_indices(arr))
        _visit(arr.type.value_field, items, path + "/", None, entries, item_select)
        return

    _hash_values(arr, present, entry.values)


def _is_unsupported_data(t: pa.DataType) -> bool:
    """Section 8: types whose schema hashes but whose data does not."""
    return (
        pa.types.is_null(t) or pa.types.is_fixed_size_list(t) or pa.types.is_map(t)
        or str(t) in _INTERVALS or str(t) in ("string_view", "binary_view")
    )


def _check_data_supported(nodes: list[tuple[str, pa.Field]]) -> None:
    """Section 7 item 3: runs before any data is hashed, whatever the number of rows."""
    for path, field in nodes:
        t = _logical(field.type)
        if _is_unsupported_data(t):
            raise ValueError(f"Hashing data of type {t} (node {path!r}) is not supported")


def _new_entries(nodes: list[tuple[str, pa.Field]]) -> dict[str, _Entry]:
    return {path: _Entry(field) for path, field in nodes}


def _feed_entries(entries: dict[str, _Entry], digest) -> None:
    """All entries in byte-wise node-path order, fed one by one (no joined copy)."""
    for path in sorted(entries):
        entries[path].feed(digest)


# ---------------------------------------------------------------------------
# Public entry points (Sections 2.5, 3.3, 4, 5)
# ---------------------------------------------------------------------------

class Digester:
    """Streaming record-batch hasher (Section 3.3)."""

    def __init__(
        self,
        schema: pa.Schema,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
    ) -> None:
        _require(schema, pa.Schema, "ArrowDigester", "a pyarrow.Schema")
        self._flags = flags_of(ordered_columns, ordered_struct_fields)
        self._include_metadata = include_metadata
        self._schema_digest = schema_digest(schema, include_metadata, self._flags)
        self._key = schema_key(schema, include_metadata, self._flags)
        nodes = schema_nodes(schema)
        _check_data_supported(nodes)
        self._entries = _new_entries(nodes)
        self._rows = 0

    def update(self, batch: pa.RecordBatch) -> None:
        _require(batch, pa.RecordBatch, "update", "a pyarrow.RecordBatch (use hash_table for a Table)")
        schema = batch.schema
        schema_nodes(schema)  # Section 7
        if schema_key(schema, self._include_metadata, self._flags) != self._key:
            raise ValueError(_SCHEMA_MISMATCH)
        # A batch rejected while it is traversed (Section 7 item 4) leaves the digester
        # exactly as it was.
        saved = {path: entry.snapshot() for path, entry in self._entries.items()}
        try:
            for i in range(batch.num_columns):
                field = schema.field(i)
                _visit(field, batch.column(i), field.name, None, self._entries)
        except BaseException:
            for path, state in saved.items():
                self._entries[path].restore(state)
            raise
        self._rows += batch.num_rows

    def finalize(self) -> bytes:
        final = hashlib.sha256(header(KIND_RECORDS, self._flags))
        final.update(self._schema_digest)
        final.update(_U64.pack(self._rows))
        _feed_entries(self._entries, final)
        return VERSION_BYTES + final.digest()


def _require(obj, cls, entry_point: str, expected: str) -> None:
    if not isinstance(obj, cls):
        raise TypeError(f"{entry_point} expects {expected}, got {type(obj).__name__}")


def hash_schema(schema: pa.Schema, *, include_metadata: bool = False, ordered_columns: bool = False,
                ordered_struct_fields: bool = False) -> bytes:
    _require(schema, pa.Schema, "hash_schema", "a pyarrow.Schema")
    return VERSION_BYTES + schema_digest(schema, include_metadata, flags_of(ordered_columns, ordered_struct_fields))


def hash_record_batch(batch: pa.RecordBatch, **options: bool) -> bytes:
    _require(batch, pa.RecordBatch, "hash_record_batch", "a pyarrow.RecordBatch (use hash_table for a Table)")
    d = Digester(batch.schema, **options)
    d.update(batch)
    return d.finalize()


def hash_table(table: pa.Table, **options: bool) -> bytes:
    _require(table, pa.Table, "hash_table", "a pyarrow.Table (use hash_record_batch for a RecordBatch)")
    d = Digester(table.schema, **options)
    for batch in table.to_batches():
        d.update(batch)
    return d.finalize()


def hash_array(array: pa.Array, *, ordered_columns: bool = False, ordered_struct_fields: bool = False) -> bytes:
    """Section 4: the array is the node at path "", always nullable.

    Its validity is always written: the logical validity after decoding dictionaries (a
    valid dictionary key that points at a null dictionary value is a null).
    """
    _require(array, (pa.Array, pa.ChunkedArray), "hash_array", "a pyarrow.Array or pyarrow.ChunkedArray")
    flags = flags_of(ordered_columns, ordered_struct_fields)
    root = pa.field("", array.type, nullable=True)
    nodes = _walk([("", root)])
    _check_data_supported(nodes)
    entries = _new_entries(nodes)
    # A ChunkedArray hashes like its chunks combined: entries are streams (Section 3.3).
    for chunk in (array.chunks if isinstance(array, pa.ChunkedArray) else [array]):
        _visit(root, chunk, "", None, entries)
    final = hashlib.sha256(header(KIND_ARRAY, flags))
    final.update(type_json(array.type, flags).encode())
    final.update(_U64.pack(len(array)))
    _feed_entries(entries, final)
    return VERSION_BYTES + final.digest()
