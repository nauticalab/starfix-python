"""[Legacy 0.4 mode] The legacy module is frozen and physically separate from format 2.

If this test fails because src/starfix/legacy/v0_4.py changed: the legacy module must not be
edited. Its output must stay byte-identical to starfix-python 0.4.0 forever (data hashed by
0.4.x stays findable). Revert the change.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

_SRC = Path(__file__).parent.parent / "src" / "starfix"
_LEGACY = _SRC / "legacy" / "v0_4.py"
_LEGACY_SHA256 = "652b0620bc484ab0ccf89a5423945244e6195d99484e1afcae48de5f4704ada3"


def test_legacy_module_is_unchanged() -> None:
    assert hashlib.sha256(_LEGACY.read_bytes()).hexdigest() == _LEGACY_SHA256, (
        "src/starfix/legacy/v0_4.py changed: the legacy 0.4 module is frozen; revert the edit"
    )


def _imported_modules(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_legacy_imports_only_stdlib_and_pyarrow() -> None:
    assert not {m for m in _imported_modules(_LEGACY) if m.startswith("starfix")}


def test_format2_does_not_import_legacy() -> None:
    for path in (_SRC / "_format2.py", _SRC / "arrow_digester.py", _SRC / "__init__.py"):
        assert not {m for m in _imported_modules(path) if m.startswith("starfix.legacy")}, path
