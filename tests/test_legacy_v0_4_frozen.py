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
_LEGACY_SHA256 = "7b4c76508f1f96b96c18e0d2bac87b9f2e8ae8b343254b9be5060e87e0fe6704"


def test_legacy_module_is_unchanged() -> None:
    assert hashlib.sha256(_LEGACY.read_bytes()).hexdigest() == _LEGACY_SHA256, (
        "src/starfix/legacy/v0_4.py changed: the legacy 0.4 module is frozen; revert the edit"
    )


def _package_of(path: Path) -> str:
    """Dotted package of a source file below src/ (e.g. src/starfix/legacy/v0_4.py -> starfix.legacy)."""
    parts = path.relative_to(_SRC.parent).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] != "__init__" else parts[:-1])


def _imported_modules(path: Path, source: str | None = None) -> set[str]:
    """Absolute dotted names a module imports: modules and module.name for each imported name.

    Relative imports are resolved against the file's package, so `from . import legacy` and
    `from .legacy.v0_4 import X` are seen as starfix.legacy / starfix.legacy.v0_4.X.
    """
    package = _package_of(path)
    names: set[str] = set()
    for node in ast.walk(ast.parse(source if source is not None else path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package.split(".")
                base = base[: len(base) - (node.level - 1)] if node.level > 1 else base
                module = ".".join(base + ([node.module] if node.module else []))
            else:
                module = node.module or ""
            names.add(module)
            names.update(f"{module}.{alias.name}" for alias in node.names)
    return names


def test_legacy_imports_only_stdlib_and_pyarrow() -> None:
    assert not {m for m in _imported_modules(_LEGACY) if m.startswith("starfix")}


def test_format2_does_not_import_legacy() -> None:
    for path in (_SRC / "_format2.py", _SRC / "arrow_digester.py", _SRC / "__init__.py"):
        assert not {m for m in _imported_modules(path) if m.startswith("starfix.legacy")}, path


def test_separation_guard_sees_relative_and_package_imports() -> None:
    """The guard itself: every ordinary way of importing the legacy module is detected."""
    fake = _SRC / "_format2.py"
    for source in (
        "from .legacy.v0_4 import ArrowDigester",
        "from . import legacy",
        "from starfix import legacy",
        "import starfix.legacy.v0_4",
        "from starfix.legacy import v0_4",
    ):
        found = {m for m in _imported_modules(fake, source) if m.startswith("starfix.legacy")}
        assert found, source
