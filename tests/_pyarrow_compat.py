"""Golden vectors that older pyarrow versions cannot decode from Arrow IPC.

pyproject allows pyarrow >= 14. Decimal32/Decimal64 IPC needs pyarrow >= 19 and the
Utf8View/BinaryView types need pyarrow >= 16. Tests that decode these vectors mark them
as strict expected failures, or leave them out, on such versions only.
"""

from __future__ import annotations

import pyarrow as pa

_NEEDS = {
    "decimal32": ({"type_decimal32_schema_f0", "type_decimal64_negative_scale_schema_f0"}, "Decimal32/Decimal64"),
    "string_view": ({"type_utf8_view_schema_f0", "type_binary_view_schema_f0"}, "Utf8View/BinaryView"),
}

UNREADABLE_HERE: dict[str, str] = {
    vid: f"pyarrow {pa.__version__} cannot read {what} IPC"
    for feature, (ids, what) in _NEEDS.items()
    if not hasattr(pa, feature)
    for vid in ids
}
