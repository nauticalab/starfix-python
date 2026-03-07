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
NULL_BYTES = b"NULL"


# ---------------------------------------------------------------------------
# Bit-vector helper (MSB-first packing, matching bitvec<u8, Msb0>)
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
# Schema / DataType serialization  (matches Rust `serialized_schema`)
# ---------------------------------------------------------------------------

def _data_type_to_value(dt: pa.DataType) -> object:
    """Convert a pyarrow DataType to the JSON-compatible value that matches
    the Rust ``data_type_to_value`` output."""
    import pyarrow as pa

    if pa.types.is_struct(dt):
        fields_json = [_inner_field_to_value(dt.field(i)) for i in range(dt.num_fields)]
        return {"Struct": fields_json}
    if pa.types.is_list(dt) or pa.types.is_large_list(dt):
        tag = "LargeList" if pa.types.is_large_list(dt) else "List"
        return {tag: _inner_field_to_value(dt.value_field)}
    if pa.types.is_fixed_size_list(dt):
        return {"FixedSizeList": [_inner_field_to_value(dt.value_field), dt.list_size]}
    if pa.types.is_map(dt):
        # pa.map_ stores a struct child called "entries"
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
        pa.utf8(): "Utf8",
        pa.large_utf8(): "LargeUtf8",
        pa.binary(): "Binary",
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
    return {
        "name": field.name,
        "data_type": _data_type_to_value(field.type),
        "nullable": field.nullable,
    }


def _raw_serde_field(field) -> dict:
    """Produce the full arrow-rs serde Field representation (used in hash_array).

    Arrow-rs Field serializes all struct fields in declaration order:
    name, data_type, nullable, dict_id, dict_is_ordered, metadata
    """
    result = OrderedDict()
    result["name"] = field.name
    result["data_type"] = _raw_serde_data_type(field.type)
    result["nullable"] = field.nullable
    result["dict_id"] = 0
    result["dict_is_ordered"] = False
    if field.metadata:
        result["metadata"] = {k.decode() if isinstance(k, bytes) else k:
                              v.decode() if isinstance(v, bytes) else v
                              for k, v in field.metadata.items()}
    else:
        result["metadata"] = {}
    return result


def _raw_serde_data_type(dt) -> object:
    """Produce the arrow-rs serde DataType representation (used in hash_array).

    This matches serde_json::to_string(&data_type) in Rust exactly.
    """
    import pyarrow as pa

    if pa.types.is_struct(dt):
        return {"Struct": [_raw_serde_field(dt.field(i)) for i in range(dt.num_fields)]}
    if pa.types.is_list(dt):
        return {"List": _raw_serde_field(dt.value_field)}
    if pa.types.is_large_list(dt):
        return {"LargeList": _raw_serde_field(dt.value_field)}
    if pa.types.is_fixed_size_list(dt):
        return {"FixedSizeList": [_raw_serde_field(dt.value_field), dt.list_size]}
    if pa.types.is_map(dt):
        return {"Map": [_raw_serde_field(dt.key_field.with_name("entries")), False]}

    return _primitive_data_type_string(dt)


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
# Field extraction  (flatten structs into BTreeMap<path, DigestBuffer>)
# ---------------------------------------------------------------------------

def _extract_fields(field: pa.Field, parent: str, out: dict[str, tuple[bool, _BitVec | None, object]]):
    import pyarrow as pa
    full_name = f"{parent}{DELIMITER}{field.name}" if parent else field.name
    if pa.types.is_struct(field.type):
        for i in range(field.type.num_fields):
            _extract_fields(field.type.field(i), full_name, out)
    else:
        if field.nullable:
            out[full_name] = (True, _BitVec(), hashlib.sha256())
        else:
            out[full_name] = (False, None, hashlib.sha256())


# ---------------------------------------------------------------------------
# Array data hashing
# ---------------------------------------------------------------------------

def _handle_null_bits(arr, bit_vec: _BitVec) -> None:
    """Push validity bits for *arr* into *bit_vec*."""
    for i in range(len(arr)):
        bit_vec.push(arr[i].is_valid)


def _hash_fixed_size_array(arr, digest_entry, element_size: int) -> None:
    """Hash a fixed-width array by reading raw buffers (matching Rust behaviour)."""
    nullable, bit_vec, data_digest = digest_entry

    # Get raw data buffer. For pyarrow, buffer index 1 is the data buffer for
    # primitive arrays (index 0 is the validity bitmap).
    bufs = arr.buffers()
    data_buf = bufs[1]
    offset = arr.offset

    raw = data_buf.to_pybytes()
    start = offset * element_size
    sliced = raw[start:]

    if not nullable:
        # Non-nullable: hash entire buffer slice for the array length
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
    nullable, bit_vec, data_digest = digest_entry

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

    Note: matches the *current* Rust implementation including the known bug
    where nullable null values feed NULL_BYTES into the data digest.
    """
    nullable, bit_vec, data_digest = digest_entry

    if not nullable:
        for i in range(len(arr)):
            val = arr[i].as_py()
            data_digest.update(struct.pack("<Q", len(val)))
            data_digest.update(val)
    else:
        # Handle null bits
        if arr.null_count > 0:
            for i in range(len(arr)):
                bit_vec.push(arr[i].is_valid)
            for i in range(len(arr)):
                if arr[i].is_valid:
                    val = arr[i].as_py()
                    data_digest.update(struct.pack("<Q", len(val)))
                    data_digest.update(val)
                else:
                    data_digest.update(NULL_BYTES)
        else:
            bit_vec.extend_true(len(arr))
            for i in range(len(arr)):
                val = arr[i].as_py()
                data_digest.update(struct.pack("<Q", len(val)))
                data_digest.update(val)


def _hash_string_array(arr, digest_entry) -> None:
    """Hash Utf8 / LargeUtf8 arrays.

    Note: matches the *current* Rust implementation including the known bug
    where nullable null values feed NULL_BYTES into the data digest.
    """
    nullable, bit_vec, data_digest = digest_entry

    if not nullable:
        for i in range(len(arr)):
            val = arr[i].as_py().encode("utf-8")
            data_digest.update(struct.pack("<Q", len(val)))
            data_digest.update(val)
    else:
        _handle_null_bits(arr, bit_vec)
        if arr.null_count > 0:
            for i in range(len(arr)):
                if arr[i].is_valid:
                    val = arr[i].as_py().encode("utf-8")
                    data_digest.update(struct.pack("<Q", len(val)))
                    data_digest.update(val)
                else:
                    data_digest.update(NULL_BYTES)
        else:
            for i in range(len(arr)):
                val = arr[i].as_py().encode("utf-8")
                data_digest.update(struct.pack("<Q", len(val)))
                data_digest.update(val)


def _update_data_digest(digest_entry, data: bytes) -> None:
    digest_entry[2].update(data)


def _hash_list_array(arr, field_data_type, digest_entry) -> None:
    import pyarrow as pa
    nullable, bit_vec, data_digest = digest_entry

    if not nullable:
        for i in range(len(arr)):
            sub = arr[i]
            sub_arr = pa.array(sub.values) if hasattr(sub, 'values') else sub
            # Actually we need the sub-list as an arrow array
            sub_arr = arr.value(i) if hasattr(arr, 'value') else arr[i].values
            data_digest.update(struct.pack("<Q", len(sub_arr)))
            _array_digest_update(field_data_type, sub_arr, digest_entry)
    else:
        _handle_null_bits(arr, bit_vec)
        if arr.null_count > 0:
            for i in range(len(arr)):
                if arr[i].is_valid:
                    sub_arr = arr.value(i) if hasattr(arr, 'value') else arr[i].values
                    data_digest.update(struct.pack("<Q", len(sub_arr)))
                    _array_digest_update(field_data_type, sub_arr, digest_entry)
        else:
            for i in range(len(arr)):
                sub_arr = arr.value(i) if hasattr(arr, 'value') else arr[i].values
                data_digest.update(struct.pack("<Q", len(sub_arr)))
                _array_digest_update(field_data_type, sub_arr, digest_entry)


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
    if pa.types.is_decimal32(dt):
        return 4
    if pa.types.is_decimal64(dt):
        return 8
    return None


def _array_digest_update(data_type, arr, digest_entry) -> None:
    import pyarrow as pa

    if pa.types.is_boolean(data_type):
        _hash_boolean_array(arr, digest_entry)
    elif pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        _hash_binary_array(arr, digest_entry)
    elif pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        _hash_string_array(arr, digest_entry)
    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        _hash_list_array(arr, data_type.value_type, digest_entry)
    elif pa.types.is_struct(data_type):
        raise NotImplementedError("Struct arrays in array_digest_update not supported")
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
    nullable, bit_vec, data_digest = entry
    if not nullable:
        final_digest.update(data_digest.digest())
    else:
        # validity bitmap length as u64 LE
        final_digest.update(struct.pack("<Q", len(bit_vec)))
        # raw backing bytes, each word as big-endian u8 (already single bytes, so identity)
        for b in bit_vec.raw_bytes():
            final_digest.update(bytes([b]))
        final_digest.update(data_digest.digest())


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
        # BTreeMap<path, (nullable, BitVec|None, sha256)> – sorted by key
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
        dt_value = _raw_serde_data_type(array.type)
        dt_json = json.dumps(dt_value, separators=(",", ":"))

        final_digest = hashlib.sha256()
        final_digest.update(dt_json.encode())

        nullable = array.null_count > 0 or (hasattr(array, 'buffers') and array.buffers()[0] is not None)
        # Match Rust: array.is_nullable() checks if the null bitmap buffer exists
        # In pyarrow, if any null exists OR the array was constructed as nullable,
        # buffers()[0] will be non-None.
        if nullable:
            entry = (True, _BitVec(), hashlib.sha256())
        else:
            entry = (False, None, hashlib.sha256())

        _array_digest_update(array.type, array, entry)
        _finalize_digest(final_digest, entry)

        return VERSION_BYTES + final_digest.digest()
