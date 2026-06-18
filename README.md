# starfix-python

Pure-Python implementation of the [starfix](https://github.com/nauticalab/starfix) Arrow logical hasher.

Produces stable SHA-256 hashes of Arrow tables, record batches, and arrays that are:

- **Column-order independent** — reordering columns does not change the hash
- **Batch-split independent** — splitting data across batches does not change the hash
- **Cross-language compatible** — identical hashes to the Rust implementation

## Installation

```bash
pip install starfix
```

## Usage

```python
import pyarrow as pa
from starfix import ArrowDigester

schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("value", pa.float64(), nullable=True),
])

# Hash a full table
table = pa.table({"id": [1, 2, 3], "value": [1.1, 2.2, 3.3]}, schema=schema)
digest = ArrowDigester.hash_table(table)

# Streaming: feed record batches incrementally
digester = ArrowDigester(schema)
for batch in batches:
    digester.update(batch)
digest = digester.finalize()
```

## Metadata hashing

By default, Arrow schema- and field-level metadata are excluded from the hash,
preserving hash format 0.0.1 stability. Pass `include_metadata=True` to any
entry point to include them:

```python
# One-shot
digest = ArrowDigester.hash_table(table, include_metadata=True)

# Streaming
digester = ArrowDigester(schema, include_metadata=True)
for batch in batches:
    digester.update(batch)
digest = digester.finalize()
```

When `include_metadata=True`, adding or changing any metadata key or value on
any field (including nested struct children and list element fields) produces a
different hash. Metadata key ordering is deterministic — the hash is stable
regardless of insertion order.

A schema with no metadata produces the same hash regardless of `include_metadata`
(empty-metadata invariant).

## License

MIT OR Apache-2.0
