"""Pure-Python implementation of the starfix Arrow logical hasher.

Produces identical hashes to the Rust implementation for all supported types.
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


def _is_list_type(dt: pa.DataType) -> bool:
    import pyarrow as pa
    return pa.types.is_list(dt) or pa.types.is_large_list(dt)


# ---------------------------------------------------------------------------
# Schema / DataType serialization  (matches Rust `serialized_schema`)
# ---------------------------------------------------------------------------

def _data_type_to_value(dt: pa.DataType) -> object:
    """Convert a pyarrow DataType to the JSON-compatible value that matches
    the Rust ``data_type_to_value`` output.

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
# DigestBufferType: (nullable, BitVec|None, structural_sha256|None, data_sha256)
# ---------------------------------------------------------------------------

def _new_digest_entry(nullable: bool, structured: bool) -> tuple:
    """Create a digest entry matching Rust DigestBufferType."""
    return (
        nullable,
        _BitVec() if nullable else None,
        hashlib.sha256() if structured else None,
        hashlib.sha256(),
    )


# ---------------------------------------------------------------------------
# Field extraction (flatten structs into BTreeMap<path, DigestBuffer>)
# ---------------------------------------------------------------------------

def _extract_fields(field: pa.Field, parent: str, out: dict[str, tuple]):
    import pyarrow as pa
    full_name = f"{parent}{DELIMITER}{field.name}" if parent else field.name
    if pa.types.is_struct(field.type):
        for i in range(field.type.num_fields):
            _extract_fields(field.type.field(i), full_name, out)
    else:
        out[full_name] = _new_digest_entry(field.nullable, _is_list_type(field.type))


# ---------------------------------------------------------------------------
# Array data hashing
# ---------------------------------------------------------------------------

def _handle_null_bits(arr, bit_vec: _BitVec) -> None:
    """Push validity bits for *arr* into *bit_vec*."""
    for i in range(len(arr)):
        bit_vec.push(arr[i].is_valid)


def _hash_fixed_size_array(arr, digest_entry, element_size: int) -> None:
    """Hash a fixed-width array by reading raw buffers (matching Rust behaviour)."""
    nullable, bit_vec, _structural, data_digest = digest_entry

    bufs = arr.buffers()
    data_buf = bufs[1]
    offset = arr.offset

    raw = data_buf.to_pybytes()
    start = offset * element_size
    sliced = raw[start:]

    if not nullable:
        end = start + len(arr) * element_size
        data_digest.update(raw[start:end])
    else:
        _handle_null_bits(arr, bit_vec)
        if arr.null_count > 0:
            for i in range(len(arr)):
                if arr[i].is_valid:
                    pos = i * element_size
                    data_digest.update(sliced[pos:pos + element_size])
        else:
            end = len(arr) * element_size
            data_digest.update(sliced[:end])


def _hash_boolean_array(arr, digest_entry) -> None:
    nullable, bit_vec, _structural, data_digest = digest_entry

    if not nullable:
        bv = _BitVec()
        for i in range(len(arr)):
            bv.push(arr[i].as_py())
        data_digest.update(bv.raw_bytes())
    else:
        _handle_null_bits(arr, bit_vec)
        bv = _BitVec()
        for i in range(len(arr)):
            if arr[i].is_valid:
                bv.push(arr[i].as_py())
        data_digest.update(bv.raw_bytes())


def _hash_binary_array(arr, digest_entry) -> None:
    """Hash Binary / LargeBinary arrays.

    Nullable null values are skipped entirely (only valid values are hashed).
    """
    nullable, bit_vec, _structural, data_digest = digest_entry

    if not nullable:
        for i in range(len(arr)):
            val = arr[i].as_py()
            data_digest.update(struct.pack("<Q", len(val)))
            data_digest.update(val)
    else:
        _handle_null_bits(arr, bit_vec)
        for i in range(len(arr)):
            if arr[i].is_valid:
                val = arr[i].as_py()
                data_digest.update(struct.pack("<Q", len(val)))
                data_digest.update(val)


def _hash_string_array(arr, digest_entry) -> None:
    """Hash Utf8 / LargeUtf8 arrays.

    Nullable null values are skipped entirely (only valid values are hashed).
    """
    nullable, bit_vec, _structural, data_digest = digest_entry

    if not nullable:
        for i in range(len(arr)):
            val = arr[i].as_py().encode("utf-8")
            data_digest.update(struct.pack("<Q", len(val)))
            data_digest.update(val)
    else:
        _handle_null_bits(arr, bit_vec)
        for i in range(len(arr)):
            if arr[i].is_valid:
                val = arr[i].as_py().encode("utf-8")
                data_digest.update(struct.pack("<Q", len(val)))
                data_digest.update(val)


def _hash_list_array(arr, field_data_type, digest_entry) -> None:
    import pyarrow as pa
    nullable, bit_vec, structural, data_digest = digest_entry

    if not nullable:
        for i in range(len(arr)):
            sub_arr = arr.value(i) if hasattr(arr, 'value') else arr[i].values
            size_bytes = struct.pack("<Q", len(sub_arr))
            # Write element count to structural digest (separating structure from leaf data)
            if structural is not None:
                structural.update(size_bytes)
            else:
                data_digest.update(size_bytes)
            _array_digest_update(field_data_type, sub_arr, digest_entry)
    else:
        _handle_null_bits(arr, bit_vec)
        for i in range(len(arr)):
            if arr[i].is_valid:
                sub_arr = arr.value(i) if hasattr(arr, 'value') else arr[i].values
                size_bytes = struct.pack("<Q", len(sub_arr))
                if structural is not None:
                    structural.update(size_bytes)
                else:
                    data_digest.update(size_bytes)
                _array_digest_update(field_data_type, sub_arr, digest_entry)


def _hash_struct_array(arr, data_type, digest_entry) -> None:
    """Hash a struct array using composite child hashing.

    Each child is independently hashed into its own DigestBufferType,
    then finalized into the parent's data stream via _finalize_child_into_data.
    """
    import pyarrow as pa

    nullable, bit_vec, _structural, data_digest = digest_entry

    # Push struct-level nulls to parent's BitVec
    if nullable:
        _handle_null_bits(arr, bit_vec)

    # Sort children alphabetically by field name
    children = [(i, data_type.field(i)) for i in range(data_type.num_fields)]
    children.sort(key=lambda x: x[1].name)

    struct_nulls = None
    if arr.null_count > 0:
        # Build struct-level null buffer from validity bitmap
        struct_nulls = [arr[i].is_valid for i in range(len(arr))]

    for idx, child_field in children:
        child_array = arr.field(idx)

        # Child is effectively nullable if the child field is nullable
        # OR the struct itself has nulls (struct-level nulls propagate down)
        effectively_nullable = child_field.nullable or (struct_nulls is not None)

        child_digest = _new_digest_entry(
            effectively_nullable,
            _is_list_type(child_field.type),
        )

        if struct_nulls is not None:
            # Propagate struct-level nulls: combined = struct AND child
            combined_child = _combine_struct_nulls_with_child(
                child_array, struct_nulls
            )
            _array_digest_update(child_field.type, combined_child, child_digest)
        else:
            _array_digest_update(child_field.type, child_array, child_digest)

        # Finalize child digest into parent's data stream
        _finalize_child_into_data(digest_entry, child_digest)


def _combine_struct_nulls_with_child(child_array, struct_nulls: list[bool]):
    """Combine struct-level nulls with child nulls and return a new array."""
    import pyarrow as pa

    # Build combined validity: struct_valid AND child_valid
    combined_valid = []
    for i in range(len(child_array)):
        child_valid = child_array[i].is_valid
        combined_valid.append(struct_nulls[i] and child_valid)

    # Reconstruct the child array with combined null mask
    # Convert to Python values and rebuild with explicit mask
    values = []
    for i in range(len(child_array)):
        if combined_valid[i]:
            values.append(child_array[i].as_py())
        else:
            values.append(None)

    return pa.array(values, type=child_array.type)


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


def _array_digest_update(data_type, arr, digest_entry) -> None:
    import pyarrow as pa

    # Normalize small variants to large equivalents
    if pa.types.is_string(data_type) and not pa.types.is_large_string(data_type):
        arr = arr.cast(pa.large_utf8())
        data_type = pa.large_utf8()
    elif pa.types.is_binary(data_type) and not pa.types.is_large_binary(data_type):
        arr = arr.cast(pa.large_binary())
        data_type = pa.large_binary()
    elif pa.types.is_list(data_type) and not pa.types.is_large_list(data_type):
        arr = arr.cast(pa.large_list(data_type.value_field))
        data_type = pa.large_list(data_type.value_field)
    elif pa.types.is_dictionary(data_type):
        arr = arr.cast(data_type.value_type)
        data_type = data_type.value_type
        # Re-enter to handle potential further normalization
        _array_digest_update(data_type, arr, digest_entry)
        return

    if pa.types.is_boolean(data_type):
        _hash_boolean_array(arr, digest_entry)
    elif pa.types.is_large_binary(data_type):
        _hash_binary_array(arr, digest_entry)
    elif pa.types.is_large_string(data_type):
        _hash_string_array(arr, digest_entry)
    elif pa.types.is_large_list(data_type):
        _hash_list_array(arr, data_type.value_type, digest_entry)
    elif pa.types.is_struct(data_type):
        _hash_struct_array(arr, data_type, digest_entry)
    else:
        element_size = _element_size_for_type(data_type)
        if element_size is not None:
            _hash_fixed_size_array(arr, digest_entry, element_size)
        else:
            raise NotImplementedError(f"Unsupported data type: {data_type}")


# ---------------------------------------------------------------------------
# Finalization helpers
# ---------------------------------------------------------------------------

def _finalize_digest(final_digest: hashlib._Hash, entry: tuple) -> None:
    nullable, bit_vec, structural, data_digest = entry
    if nullable:
        # Validity bitmap length as u64 LE
        final_digest.update(struct.pack("<Q", len(bit_vec)))
        # Raw backing bytes, each word as big-endian u8 (already single bytes)
        for b in bit_vec.raw_bytes():
            final_digest.update(bytes([b]))
    # Structural digest (if list type)
    if structural is not None:
        final_digest.update(structural.digest())
    # Data/leaf digest
    final_digest.update(data_digest.digest())


def _finalize_child_into_data(parent_entry: tuple, child_entry: tuple) -> None:
    """Finalize a child's digest and write the resulting bytes into the parent's data stream.

    Used for composite types (structs) where each child is independently hashed
    and then its finalized representation is fed into the parent digest.
    """
    _p_nullable, _p_bit_vec, _p_structural, parent_data = parent_entry
    c_nullable, c_bit_vec, c_structural, c_data = child_entry

    # Null bits first (if nullable child)
    if c_nullable and c_bit_vec is not None:
        parent_data.update(struct.pack("<Q", len(c_bit_vec)))
        for b in c_bit_vec.raw_bytes():
            parent_data.update(bytes([b]))
    # Structural digest (if list child)
    if c_structural is not None:
        parent_data.update(c_structural.digest())
    # Data/leaf digest
    parent_data.update(c_data.digest())


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
        # BTreeMap<path, (nullable, BitVec|None, structural|None, sha256)> – sorted by key
        self._fields: dict[str, tuple] = {}
        for i in range(len(schema)):
            _extract_fields(schema.field(i), "", self._fields)
        # Ensure sorted order (Python 3.7+ dicts are insertion-ordered)
        self._fields = dict(sorted(self._fields.items()))

    def update(self, record_batch: pa.RecordBatch) -> None:
        """Feed a RecordBatch into the running digest."""
        import pyarrow as pa

        for field_path, entry in self._fields.items():
            parts = field_path.split(DELIMITER)
            if len(parts) == 1:
                col_name = parts[0]
                col_idx = record_batch.schema.get_field_index(col_name)
                col = record_batch.column(col_idx)
                field = record_batch.schema.field(col_idx)
                _array_digest_update(field.type, col, entry)
            else:
                # Nested struct traversal
                col_idx = record_batch.schema.get_field_index(parts[0])
                arr = record_batch.column(col_idx)
                for level in range(1, len(parts) - 1):
                    struct_arr = arr
                    child_idx = struct_arr.type.get_field_index(parts[level])
                    arr = struct_arr.field(child_idx)
                leaf_name = parts[-1]
                child_idx = arr.type.get_field_index(leaf_name)
                leaf_arr = arr.field(child_idx)
                leaf_dt = arr.type.field(child_idx).type
                _array_digest_update(leaf_dt, leaf_arr, entry)

    def finalize(self) -> bytes:
        """Consume the digester and return the versioned hash."""
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
        """Hash a single array (matches Rust ``hash_array``)."""
        import pyarrow as pa

        # Resolve dictionary arrays to their plain value type
        effective_type = array.type
        effective_array = array
        if pa.types.is_dictionary(effective_type):
            effective_type = effective_type.value_type
            effective_array = array.cast(effective_type)

        # Normalize to canonical large types
        normalized_type = _normalize_data_type(effective_type)

        # Use data_type_to_value for canonical metadata serialization
        dt_value = _data_type_to_value(normalized_type)
        dt_json = json.dumps(dt_value, separators=(",", ":"))

        final_digest = hashlib.sha256()
        final_digest.update(dt_json.encode())

        # Match Rust: is_nullable() checks null_count > 0, not buffer existence.
        # Arrays with all-valid values but a null bitmap (e.g. from dictionary cast)
        # are treated as non-nullable.
        nullable = effective_array.null_count > 0

        entry = _new_digest_entry(nullable, _is_list_type(normalized_type))
        _array_digest_update(effective_type, effective_array, entry)
        _finalize_digest(final_digest, entry)

        return VERSION_BYTES + final_digest.digest()


# ---------------------------------------------------------------------------
# Type normalization (matches Rust normalize_data_type / normalize_field)
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
        inner = _normalize_field(dt.key_field)
        return pa.map_(inner.type, dt.item_field.type)
    return dt


def _normalize_field(field: pa.Field) -> pa.Field:
    """Normalize a single field: keep name and nullability, normalize the data type."""
    import pyarrow as pa
    return pa.field(field.name, _normalize_data_type(field.type), field.nullable)
