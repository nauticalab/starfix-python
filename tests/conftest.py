from __future__ import annotations

import pyarrow as pa


def pytest_report_header() -> str:
    """The pyarrow the tests actually import (CI pins several versions)."""
    return f"pyarrow {pa.__version__} ({pa.__file__})"
