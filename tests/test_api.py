"""Public API shape of ArrowDigester (hash format 2)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from starfix import ArrowDigester

OPTIONS = ("include_metadata", "ordered_columns", "ordered_struct_fields")


def _inputs():
    schema = pa.schema([pa.field("a", pa.int32())])
    batch = pa.record_batch([pa.array([1], pa.int32())], schema=schema)
    return schema, batch, pa.Table.from_batches([batch]), batch.column(0)


def test_options_are_keyword_only() -> None:
    schema, batch, table, arr = _inputs()
    with pytest.raises(TypeError):
        ArrowDigester(schema, True)  # type: ignore[misc]
    for call, arg in ((ArrowDigester.hash_schema, schema), (ArrowDigester.hash_record_batch, batch),
                      (ArrowDigester.hash_table, table)):
        with pytest.raises(TypeError):
            call(arg, True)  # type: ignore[misc]
        for name in OPTIONS:
            assert call(arg, **{name: True}).hex().startswith("000002")
    with pytest.raises(TypeError):
        ArrowDigester.hash_array(arr, True)  # type: ignore[misc]


def test_hash_array_has_no_include_metadata() -> None:
    with pytest.raises(TypeError):
        ArrowDigester.hash_array(pa.array([1]), include_metadata=True)  # type: ignore[call-arg]


def test_no_third_option_in_format_2() -> None:
    schema, *_ = _inputs()
    with pytest.raises(TypeError):
        ArrowDigester.hash_schema(schema, nested_validity=True)  # type: ignore[call-arg]


def test_streaming_equals_one_shot() -> None:
    schema, batch, table, _ = _inputs()
    d = ArrowDigester(schema, ordered_columns=True)
    d.update(batch)
    assert d.finalize() == ArrowDigester.hash_table(table, ordered_columns=True)
    assert ArrowDigester.hash_record_batch(batch, ordered_columns=True) == ArrowDigester.hash_table(
        table, ordered_columns=True
    )
