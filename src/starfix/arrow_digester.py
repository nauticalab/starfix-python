"""Pure-Python implementation of the starfix Arrow logical hasher.

Implements hash format 2 of the byte-layout specification defined in the starfix Rust
crate (``nauticalab/starfix docs/byte-layout-spec.md``). Every hash is 35 bytes: the
3-byte format number ``00 00 02`` followed by a SHA-256 digest, and is byte-identical to
the Rust implementation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from starfix import _format2

if TYPE_CHECKING:
    import pyarrow as pa

VERSION_BYTES = _format2.VERSION_BYTES


class ArrowDigester:
    """Hash Arrow schemas, record batches, tables and arrays (hash format 2).

    Hashes ignore the order of the top-level columns and of struct fields (at every
    depth) unless ``ordered_columns`` / ``ordered_struct_fields`` is set; nothing else is
    reordered. Arrow schema- and field-level metadata are hashed only with
    ``include_metadata=True``; a schema without metadata hashes the same either way.

    Raises ``ValueError`` (in every mode) for: schemas in which two nodes share a path
    (duplicate names, or a name containing ``/`` that aliases another node); ListView,
    LargeListView, Union and RunEndEncoded types; hashing data of a schema containing
    Null, FixedSizeList, Map, Interval or view types (their schemas hash), whatever the
    number of rows; a non-nullable field holding a null that no enclosing struct null
    explains; duplicate metadata keys when ``include_metadata`` is on; and a streamed
    batch whose schema differs from the digester's. Raises ``TypeError`` for a wrong
    container type (for example a Table passed to ``hash_record_batch``).

    A digester is not thread-safe: feed one digester from one thread at a time. A batch
    rejected by ``update`` leaves the digester unchanged.
    """

    def __init__(
        self,
        schema: pa.Schema,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
    ) -> None:
        """Start a streaming hash of record batches with this schema.

        Args:
            schema: The Arrow schema every record batch must have.
            include_metadata: Hash schema- and field-level metadata.
            ordered_columns: Make the order of the top-level columns part of the hash.
            ordered_struct_fields: Make the order of struct fields, at every depth,
                part of the hash.

        Hashes taken with different order options never coincide.
        """
        self._digester = _format2.Digester(
            schema,
            include_metadata=include_metadata,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
        )

    def update(self, record_batch: pa.RecordBatch) -> None:
        """Add a record batch. Splitting rows into batches does not change the hash."""
        self._digester.update(record_batch)

    def finalize(self) -> bytes:
        """Return the 35-byte hash of everything added so far."""
        return self._digester.finalize()

    @staticmethod
    def hash_schema(
        schema: pa.Schema,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
    ) -> bytes:
        """Hash an Arrow schema."""
        return _format2.hash_schema(
            schema,
            include_metadata=include_metadata,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
        )

    @staticmethod
    def hash_record_batch(
        record_batch: pa.RecordBatch,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
    ) -> bytes:
        """Hash one record batch (equal to streaming it through a digester)."""
        return _format2.hash_record_batch(
            record_batch,
            include_metadata=include_metadata,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
        )

    @staticmethod
    def hash_table(
        table: pa.Table,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
    ) -> bytes:
        """Hash a table: a digester over ``table.to_batches()``."""
        return _format2.hash_table(
            table,
            include_metadata=include_metadata,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
        )

    @staticmethod
    def hash_array(
        array: pa.Array,
        *,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
    ) -> bytes:
        """Hash a single array or ChunkedArray (equal to its chunks combined).

        There is deliberately no ``include_metadata``: an array has no schema or field
        metadata. ``ordered_columns`` changes only the header of an array's hash.
        """
        return _format2.hash_array(
            array, ordered_columns=ordered_columns, ordered_struct_fields=ordered_struct_fields
        )
