"""Shared task file selection for inspection, packaging and fingerprints."""

from __future__ import annotations

import fnmatch
import os
from collections.abc import Iterator
from pathlib import Path

SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    ".tox",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "dist",
    "build",
    ".ssh",
    ".aws",
    ".azure",
}
SECRET_PATTERNS = (
    ".env",
    ".env.*",
    ".envrc",
    ".npmrc",
    ".pypirc",
    ".netrc",
    "*.pem",
    "*.key",
    "*.p12",
    "*.pfx",
    "id_rsa*",
    "id_ed25519*",
    "id_ecdsa*",
)


def task_files(root: Path, excluded: tuple[Path, ...] = ()) -> Iterator[Path]:
    excluded = tuple(path.resolve() for path in excluded)
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(
            name
            for name in dirs
            if name not in SKIP_DIRS
            and not (Path(directory) / name).is_symlink()
            and not name.endswith(".egg-info")
        )
        for name in sorted(files):
            path = Path(directory) / name
            if (
                path.is_symlink()
                or path.resolve() in excluded
                or any(fnmatch.fnmatch(name.lower(), p) for p in SECRET_PATTERNS)
                or name.endswith((".pyc", ".pyo"))
            ):
                continue
            yield path
