"""Pure-Python implementation of the starfix Arrow logical hasher.

Implements the byte-layout specification defined in the starfix Rust crate
(``nauticalab/starfix docs/byte-layout-spec.md``).
"""

from __future__ import annotations

import hashlib
import json
import struct
from collections import OrderedDict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

VERSION_BYTES = b"\x00\x00\x01"
DELIMITER = "/"


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

def _data_type_to_value(dt: pa.DataType) -> object:
    """Convert a pyarrow DataType to the JSON-compatible value that matches
    the canonical form described in spec Section 2.1.

    Types are normalized: Utf8→LargeUtf8, Binary→LargeBinary, List→LargeList,
    Dictionary→value_type. Struct fields are sorted alphabetically.
    """
    import pyarrow as pa

    # Normalize: Dictionary → recurse on value type
    if pa.types.is_dictionary(dt):
        return _data_type_to_value(dt.value_type)

    if pa.types.is_struct(dt):
        # Sort struct fields alphabetically by name
        fields = [dt.field(i) for i in range(dt.num_fields)]
        fields.sort(key=lambda f: f.name)
        fields_json = [_inner_field_to_value(f) for f in fields]
        return {"Struct": fields_json}
    if pa.types.is_list(dt) or pa.types.is_large_list(dt):
        return {"LargeList": _element_type_to_value(dt.value_field)}
    if pa.types.is_fixed_size_list(dt):
        return {"FixedSizeList": [_element_type_to_value(dt.value_field), dt.list_size]}
    if pa.types.is_map(dt):
        return {"Map": [_inner_field_to_value(dt.key_field.with_name("entries")), False]}

    # Primitive / leaf types – must match Arrow-Rust serde
    return _primitive_data_type_string(dt)


def _primitive_data_type_string(dt: pa.DataType) -> object:
    """Return the serde_json representation that arrow-rs produces."""
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

    raise NotImplementedError(f"Unsupported data type: {dt}")


def _inner_field_to_value(field: pa.Field) -> dict:
    """Convert a field to JSON with name, data_type, and nullable."""
    return {
        "name": field.name,
        "data_type": _data_type_to_value(field.type),
        "nullable": field.nullable,
    }


def _element_type_to_value(field: pa.Field) -> dict:
    """Convert a container element field to JSON with only data_type and nullable (no name).

    Used for list and fixed-size list element types, matching Rust ``element_type_to_value``.
    """
    return {
        "data_type": _data_type_to_value(field.type),
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
    return json.dumps(sorted_fields, separators=(",", ":"))


def _hash_schema(schema: pa.Schema) -> bytes:
    return hashlib.sha256(_serialized_schema(schema).encode()).digest()


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
) -> None:
    """Recursively decompose a data type into BTreeMap entries.

    This implements the recursive decomposition described in spec Section 3:
    - Structs are transparent — no entry, recurse into sorted children
    - Lists create validity-only + structural/data entries
    - Leaves create data entries
    """
    import pyarrow as pa
    canonical = _normalize_data_type(data_type)

    if pa.types.is_struct(canonical):
        # Struct is transparent — no entry for the struct itself.
        # Recurse into children sorted alphabetically (spec Section 3.5).
        children = [canonical.field(i) for i in range(canonical.num_fields)]
        children.sort(key=lambda f: f.name)
        for child in children:
            child_path = f"{path}{DELIMITER}{child.name}" if path else child.name
            _extract_type_entries(child.type, child.nullable, child_path, out)

    elif pa.types.is_large_list(canonical) or pa.types.is_list(canonical):
        # Nullable list: validity-only entry at `path` (spec Section 3.4)
        if nullable:
            out[path] = _new_validity_only()

        # List level: entry at path + "/" (spec Section 3.4)
        list_path = f"{path}{DELIMITER}"
        inner_field = canonical.value_field
        inner_canonical = _normalize_data_type(inner_field.type)

        if pa.types.is_struct(inner_canonical):
            # List<Struct>: structural-only at list_path, struct children get own entries
            out[list_path] = _new_structural_only(inner_field.nullable)
            _extract_type_entries(inner_field.type, inner_field.nullable, list_path, out)
        elif pa.types.is_large_list(inner_canonical) or pa.types.is_list(inner_canonical):
            # List<List>: structural-only, recurse into inner list
            out[list_path] = _new_structural_only(inner_field.nullable)
            _extract_type_entries(inner_field.type, inner_field.nullable, list_path, out)
        else:
            # List<Leaf>: list-leaf entry with both structural + data
            out[list_path] = _new_list_leaf(inner_field.nullable)

    else:
        # Leaf type: data entry
        out[path] = _new_data_only(nullable)


def _extract_fields(field: pa.Field, parent: str, out: dict[str, tuple]) -> None:
    """Extract BTreeMap entries from a schema field (record-batch path)."""
    full_name = f"{parent}{DELIMITER}{field.name}" if parent else field.name
    _extract_type_entries(field.type, field.nullable, full_name, out)


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


def _traverse_and_update(data_type, nullable, array, path, ancestor_nulls, fields):
    """Top-down recursive traversal that routes data to BTreeMap entries.

    Parameters:
        data_type: Arrow data type of the array
        nullable: whether this position is nullable
        array: the Arrow array to hash
        path: current BTreeMap key path
        ancestor_nulls: list of bools from ancestor struct nulls, or None
        fields: the BTreeMap of entries to populate
    """
    import pyarrow as pa

    effective_type, effective_array = _normalize_array(data_type, array)
    canonical = _normalize_data_type(effective_type)

    if pa.types.is_large_list(canonical):
        _traverse_list(effective_array, canonical.value_field, nullable, path, ancestor_nulls, fields)
    elif pa.types.is_struct(canonical):
        _traverse_struct(effective_array, nullable, path, ancestor_nulls, fields)
    else:
        _traverse_leaf(effective_type, effective_array, path, ancestor_nulls, fields)


def _traverse_list(array, value_field, nullable, path, ancestor_nulls, fields):
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
            )


def _traverse_struct(array, nullable, path, ancestor_nulls, fields):
    """Traverse a struct array — struct is transparent (spec Section 3.5).

    Struct-level nulls are AND-propagated to all descendant entries.
    """
    import pyarrow as pa

    struct_array = array
    # Combine struct's own nulls with ancestor nulls (AND propagation)
    if nullable:
        combined = _combine_null_masks(_get_validity_list(struct_array), ancestor_nulls)
    else:
        combined = ancestor_nulls

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
        )


def _traverse_leaf(data_type, array, path, ancestor_nulls, fields):
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
    _hash_leaf_data(data_type, array, data_digest, effective)


def _hash_leaf_data(data_type, array, data_digest, effective_nulls):
    """Hash leaf-level data bytes into the data digest (spec Sections 3.1–3.3)."""
    import pyarrow as pa

    if pa.types.is_boolean(data_type):
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

    Produces identical SHA-256 hashes with a 3-byte version prefix.
    """

    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema
        self._schema_digest = _hash_schema(schema)
        # BTreeMap<path, (BitVec|None, sha256|None, sha256|None)> — sorted by key
        self._fields: dict[str, tuple] = {}
        for i in range(len(schema)):
            _extract_fields(schema.field(i), "", self._fields)
        # Ensure sorted order (Python 3.7+ dicts are insertion-ordered)
        self._fields = dict(sorted(self._fields.items()))

    def update(self, record_batch: pa.RecordBatch) -> None:
        """Feed a RecordBatch into the running digest (spec Sections 3–5)."""
        # Build a mapping from top-level column name to (field, array)
        schema = record_batch.schema
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
            )

    def finalize(self) -> bytes:
        """Consume the digester and return the versioned hash (spec Section 5)."""
        final_digest = hashlib.sha256()
        final_digest.update(self._schema_digest)
        for _path, entry in sorted(self._fields.items()):
            _finalize_digest(final_digest, entry)
        return VERSION_BYTES + final_digest.digest()

    # -- Convenience class methods ------------------------------------------

    @staticmethod
    def hash_schema(schema: pa.Schema) -> bytes:
        return VERSION_BYTES + _hash_schema(schema)

    @staticmethod
    def hash_record_batch(record_batch: pa.RecordBatch) -> bytes:
        d = ArrowDigester(record_batch.schema)
        d.update(record_batch)
        return d.finalize()

    @staticmethod
    def hash_table(table: pa.Table) -> bytes:
        """Hash a full table (iterates over all batches)."""
        d = ArrowDigester(table.schema)
        for batch in table.to_batches():
            d.update(batch)
        return d.finalize()

    @staticmethod
    def hash_array(array: pa.Array) -> bytes:
        """Hash a single array (spec Section 6).

        Uses the same recursive BTreeMap decomposition as the record-batch path.
        """
        import pyarrow as pa

        # Resolve dictionary arrays to their plain value type
        effective_type = array.type
        effective_array = array
        if pa.types.is_dictionary(effective_type):
            effective_type = effective_type.value_type
            effective_array = array.cast(effective_type)

        # Normalize to canonical large types
        normalized_type = _normalize_data_type(effective_type)

        # Step 1: Type metadata (canonical JSON string)
        dt_value = _data_type_to_value(normalized_type)
        dt_value = _sort_json_value(dt_value)
        dt_json = json.dumps(dt_value, separators=(",", ":"))

        final_digest = hashlib.sha256()
        final_digest.update(dt_json.encode())

        # Determine nullability: arrays with null_count > 0 are nullable
        nullable = effective_array.null_count > 0

        # Step 2: Build BTreeMap entries from the type tree (same as record-batch)
        fields: dict[str, tuple] = {}
        _extract_type_entries(effective_type, nullable, "", fields)
        fields = dict(sorted(fields.items()))

        # Step 3: Traverse and populate entries
        _traverse_and_update(
            effective_type,
            nullable,
            effective_array,
            "",
            None,
            fields,
        )

        # Step 4: Finalize all entries into the digest
        for _path, entry in sorted(fields.items()):
            _finalize_digest(final_digest, entry)

        return VERSION_BYTES + final_digest.digest()
