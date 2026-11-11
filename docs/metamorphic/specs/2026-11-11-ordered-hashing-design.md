# Ordered and Nested-Validity Hashing Options (Python)

**Date:** 2026-11-11
**Issue:** ITL-898
**Repos:** nauticalab/starfix (Rust, authoritative), nauticalab/starfix-python (this repo)
**Status:** Provisional: the Rust design is not yet approved, and the third option's name may change.

---

## Source of truth

The byte layout is defined in the Rust repository: `docs/byte-layout-spec.md` Section 9
(options and flags byte, options tag, ordered Phase 1 JSON, `hash_array` type JSON, nested
validity, Boolean layout under the third option, node-path rejection, streaming, invariants,
worked examples T1 to T6, golden-file format). The design rationale is in the Rust repo at
`docs/metamorphic/specs/2026-11-11-ordered-hashing-design.md`. This document records only the
Python-specific decisions.

The golden values are produced by Rust (`cargo run --bin emit_golden_ordered`) and copied here
byte for byte as `tests/golden/ordered_v0.5.json`. Python never generates or edits them.

## API

Keyword-only arguments with the Rust `HasherConfig` field names, all `False` by default:

```python
ArrowDigester(schema, *, include_metadata=False, ordered_columns=False,
              ordered_struct_fields=False, nested_validity=False)
ArrowDigester.hash_schema(schema, *, include_metadata=..., ordered_columns=..., ordered_struct_fields=..., nested_validity=...)
ArrowDigester.hash_record_batch(batch, *, ...same four...)
ArrowDigester.hash_table(table, *, ...same four...)
ArrowDigester.hash_array(array, *, ordered_columns=..., ordered_struct_fields=..., nested_validity=...)
```

Rejections (spec 9.6, 9.7) raise `ValueError`.

**Renaming an option:** `_OPTION_FLAGS` in `src/starfix/arrow_digester.py` is the single table
of option names and flag bits. A rename changes that table, the keyword in the five
signatures, their docstrings, the README, and the name constants at the top of
`tests/test_ordered_hashing.py`. The golden parity test reads option keys from `_OPTION_FLAGS`.
The name is not hashed, so no hash moves.

## Implementation notes

* `flags == 0` takes exactly the previous code paths (`_serialized_schema`, the old
  `hash_array` type JSON, the bit-packed Boolean layout, no checks), so every existing hash is
  unchanged (invariant I1). All existing tests and golden files pass unmodified.
* With `flags != 0`, JSON is written with raw UTF-8 (`ensure_ascii=False`), as serde_json does,
  and `Map` is serialized as the Rust form `{"Map": [<entries field>, keys_sorted]}`.
* Nested validity threads one boolean through entry extraction and traversal. Booleans are fed
  as one byte per valid value under the third option (spec 9.5.1).

## Known default-mode divergences from Rust (not changed here)

Found while implementing from the spec. All three predate ITL-898 and are present in the
released starfix-python 0.4.0. Fixing them moves released hashes, or rejects batches that are
accepted today, which is the developer's decision. `tests/test_known_divergences.py` pins the
Rust behaviour as `xfail(strict=True)` tests; the four affected golden vectors
(`map_struct_ab_schema_f0`, `map_struct_ba_schema_f0`, `names_array_struct_f0`,
`names_array_struct_int8_f0`) are marked the same way in `tests/test_golden_parity_ordered.py`.

| Input | Python today (flags 0) | Rust |
|---|---|---|
| `hash_schema` of a `Map` column | `{"Map":[{"data_type":<key type>,"name":"entries","nullable":false},false]}`: the value type and `keys_sorted` are ignored | `{"Map":[{"data_type":{"Struct":[key, value]},"name":"entries","nullable":false},keys_sorted]}` |
| `hash_array` of a struct with a non-ASCII child name `é` | type JSON escapes it as the six ASCII characters `\u00e9` | raw UTF-8 (bytes `c3 a9`), spec 2.1 JSON text rules |
| `update` with a batch whose schema differs (spec 9.7, no options) | accepted and hashed as given | rejected: Phase 1 JSON must match, plus the Phase 2 document with `include_metadata` |

With any option on, Python matches Rust in all three cases.

## Version

Proposed: 0.5.0, matching the Rust crate (new keyword arguments, new hash modes). The version
comes from git tags (hatch-vcs); nothing is bumped in this change.
