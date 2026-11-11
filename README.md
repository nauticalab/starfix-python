# starfix-python

Pure-Python implementation of the [starfix](https://github.com/nauticalab/starfix) Arrow logical hasher.

Produces stable SHA-256 hashes of Arrow tables, record batches, and arrays that are:

- **Column-order independent** by default — reordering columns or struct fields does not change the hash (opt-in order sensitivity: see [Hashing options](#hashing-options-order-sensitivity-and-nested-validity))
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

## Hashing options: order sensitivity and nested validity

Three further keyword-only options, all `False` by default, are accepted by
`ArrowDigester(...)`, `hash_schema`, `hash_record_batch`, `hash_table` and
`hash_array`. They have the same names as the Rust `HasherConfig` fields and
produce byte-identical hashes (layout: starfix `docs/byte-layout-spec.md`,
Section 9).

| Option | Effect |
|---|---|
| `ordered_columns` | The order of the top-level columns is part of the hash. |
| `ordered_struct_fields` | The order of struct fields, at every nesting depth (inside lists, struct children, …), is part of the hash. |
| `nested_validity` | Every nullable struct, and every nullable list or struct that is a list element, records its own validity. A null struct then differs from a struct whose fields are all null, null struct rows are counted, and the outer lengths of a list of nullable lists are hashed. |

```python
# A pa.Table whose column order and struct field order are part of its identity
digest = ArrowDigester.hash_table(
    table, ordered_columns=True, ordered_struct_fields=True, nested_validity=True
)

# hash_array takes the same options (ordered_columns has no effect on an array's
# layout, but like every option it is part of the hash)
digest = ArrowDigester.hash_array(struct_array, ordered_struct_fields=True)
```

When to use them:

- Leave them off to identify *data* independent of how columns or struct fields
  happen to be ordered (the default; every existing hash is unchanged).
- Turn on `ordered_columns` / `ordered_struct_fields` when order is part of the
  value's identity, as for a DataFrame or a `pa.StructArray` whose type
  `struct<a, b>` differs from `struct<b, a>`.
- Turn on `nested_validity` when null structs and null nested lists must be
  distinguished from their non-null look-alikes.

Things to know:

- **A hash taken with any option on never equals a hash taken with a different
  set of options**, in particular with all options off, even when order cannot
  matter (for example, a one-column table). Choose the options once per use case.
- With any option on, a schema in which two nodes share a path is rejected with
  `ValueError`: duplicate column names, duplicate field names within a struct,
  or a name containing `/` that aliases another node (a column `"a/b"` next to a
  struct column `a` with a child `b`). A `/` that aliases nothing (`"km/h"`) is
  fine.
- Streaming with any option on: every batch passed to `update` must have the
  digester's schema under those options, otherwise `ValueError` is raised. With
  `ordered_columns`, a batch with the same columns in another order is rejected;
  without it, the batch is accepted. The same holds for struct field order and
  `ordered_struct_fields`.
- `include_metadata` works with every option as described above.

## License

MIT OR Apache-2.0
