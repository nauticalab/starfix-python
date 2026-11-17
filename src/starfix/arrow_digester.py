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
    Null, Map, Interval or view types (their schemas hash; also inside a FixedSizeList),
    whatever the number of rows; a non-nullable field holding a null that no enclosing
    struct null explains; duplicate metadata keys when ``include_metadata`` is on; and a streamed
    batch whose schema differs from the digester's. Raises ``TypeError`` for a wrong
    container type (for example a Table passed to ``hash_record_batch``).

    A digester is not thread-safe: feed one digester from one thread at a time. A batch
    rejected by ``update`` leaves the digester unchanged.

    To hash many tables or batches of one schema, analyse the schema once with
    ``ArrowDigester.prepare`` and hash through the returned ``PreparedSchema``.
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

    @classmethod
    def _from_analysis(cls, analysis: _format2.SchemaAnalysis) -> ArrowDigester:
        digester = cls.__new__(cls)
        digester._digester = _format2.Digester.from_analysis(analysis)
        return digester

    @staticmethod
    def prepare(
        schema: pa.Schema,
        *,
        include_metadata: bool = False,
        ordered_columns: bool = False,
        ordered_struct_fields: bool = False,
    ) -> PreparedSchema:
        """Analyse a schema once, for hashing many tables or record batches of it.

        Returns an immutable ``PreparedSchema`` owned by the caller; its hashes equal the
        one-shot calls with the same options. Raises what ``hash_schema`` raises. A schema
        whose data cannot be hashed (Null, Map, Interval, view types) still prepares, for its
        ``schema_digest``; hashing data with it raises ``ValueError``.

        Args:
            schema: The Arrow schema of the data to hash.
            include_metadata: Hash schema- and field-level metadata.
            ordered_columns: Make the order of the top-level columns part of the hash.
            ordered_struct_fields: Make the order of struct fields, at every depth,
                part of the hash.
        """
        analysis = _format2.analyse(
            schema,
            "prepare",
            include_metadata=include_metadata,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
        )
        return PreparedSchema._create(schema, analysis)

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


class PreparedSchema:
    """A schema analysed once, with its hashing options (create with ``ArrowDigester.prepare``).

    Hashes many tables or record batches of the schema without analysing it again; every
    hash is byte-identical to the one-shot ``ArrowDigester`` call with the same options.
    Immutable and owned by the caller: starfix keeps no cache or other state of its own. Safe
    to share between threads (each hash or ``digester()`` has its own state).

    **Same-schema rule.** A table or record batch is accepted exactly when its schema has the
    prepared schema's canonical schema key under the prepared options, i.e. when
    ``ArrowDigester.hash_schema(its_schema, **options) == prepared.schema_digest``: the same
    column names, logical types and nullability at every depth (column order when
    ``ordered_columns``, struct field order when ``ordered_struct_fields``) and, with
    ``include_metadata``, the same field metadata at every node, schema metadata and extension
    type names and serializations. This is the rule ``ArrowDigester.update`` applies to
    streamed batches. Anything else raises ``ValueError``; nothing is hashed with another
    schema's analysis. An input whose schema is identical to the prepared one (equal Arrow
    IPC serialization) is accepted by a byte comparison; other inputs, such as a ``string``
    column for a prepared ``large_string`` one, are checked by computing their schema key.
    """

    __slots__ = ("_analysis", "_schema")

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise TypeError("PreparedSchema is created by ArrowDigester.prepare(schema, ...)")

    @classmethod
    def _create(cls, schema: pa.Schema, analysis: _format2.SchemaAnalysis) -> PreparedSchema:
        prepared = object.__new__(cls)
        object.__setattr__(prepared, "_schema", schema)
        object.__setattr__(prepared, "_analysis", analysis)
        return prepared

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"PreparedSchema is immutable (cannot set {name!r})")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"PreparedSchema is immutable (cannot delete {name!r})")

    def __copy__(self) -> PreparedSchema:
        return self

    def __deepcopy__(self, memo: dict) -> PreparedSchema:
        return self

    def __reduce__(self) -> tuple:
        # Pickled as (schema, options) and prepared again when loaded.
        return (_prepare, (self._schema, self.include_metadata, self.ordered_columns, self.ordered_struct_fields))

    def __repr__(self) -> str:
        return (
            f"PreparedSchema(include_metadata={self.include_metadata}, ordered_columns={self.ordered_columns}, "
            f"ordered_struct_fields={self.ordered_struct_fields}, schema_digest={self.schema_digest.hex()})"
        )

    @property
    def schema(self) -> pa.Schema:
        """The prepared Arrow schema."""
        return self._schema

    @property
    def include_metadata(self) -> bool:
        return self._analysis.include_metadata

    @property
    def ordered_columns(self) -> bool:
        return bool(self._analysis.flags & _format2.ORDERED_COLUMNS)

    @property
    def ordered_struct_fields(self) -> bool:
        return bool(self._analysis.flags & _format2.ORDERED_STRUCT_FIELDS)

    @property
    def schema_digest(self) -> bytes:
        """The 35-byte schema hash, equal to ``ArrowDigester.hash_schema`` with the same options."""
        return VERSION_BYTES + self._analysis.digest

    def hash_record_batch(self, record_batch: pa.RecordBatch) -> bytes:
        """Hash one record batch of this schema (see the same-schema rule)."""
        return _format2.hash_prepared_record_batch(self._analysis, record_batch)

    def hash_table(self, table: pa.Table) -> bytes:
        """Hash a table of this schema (see the same-schema rule); checked once per table."""
        return _format2.hash_prepared_table(self._analysis, table)

    def digester(self) -> ArrowDigester:
        """A fresh streaming ``ArrowDigester`` for this schema and options, without analysis."""
        return ArrowDigester._from_analysis(self._analysis)


def _prepare(schema: pa.Schema, include_metadata: bool, ordered_columns: bool, ordered_struct_fields: bool) -> PreparedSchema:
    return ArrowDigester.prepare(
        schema,
        include_metadata=include_metadata,
        ordered_columns=ordered_columns,
        ordered_struct_fields=ordered_struct_fields,
    )
