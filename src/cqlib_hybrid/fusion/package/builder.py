"""Build fusion task zip packages."""

from __future__ import annotations

import os
import tempfile
import zipfile
from pathlib import Path

from cqlib_hybrid.fusion.client._archive import validate_archive
from cqlib_hybrid.fusion.exceptions import CqfusionRequestError, PackageValidationError
from cqlib_hybrid.fusion.package.files import task_files
from cqlib_hybrid.fusion.package.validator import validate_package_dir

MAX_PACKAGE_BYTES = 100 * 1024 * 1024


def build_package(source_dir: Path | str, output_zip: Path | str) -> Path:
    root = Path(source_dir).resolve()
    report = validate_package_dir(root)
    if not report.ok:
        raise PackageValidationError("; ".join(report.errors))

    out = Path(output_zip).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    files: list[Path] = []
    source_bytes = 0
    for path in task_files(root, excluded=(out,)):
        source_bytes += path.stat().st_size
        if source_bytes > MAX_PACKAGE_BYTES:
            raise PackageValidationError(
                f"package source exceeds {MAX_PACKAGE_BYTES} bytes limit "
                f"({source_bytes} bytes)"
            )
        files.append(path)

    fd, name = tempfile.mkstemp(dir=out.parent, prefix=".task-", suffix=".zip")
    os.close(fd)
    temporary = Path(name)
    try:
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in files:
                archive.write(path, arcname=path.relative_to(root).as_posix())
        try:
            validate_archive(temporary)
        except CqfusionRequestError as exc:
            raise PackageValidationError(str(exc)) from exc
        os.replace(temporary, out)
    finally:
        temporary.unlink(missing_ok=True)
    return out
