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

## License

MIT OR Apache-2.0
