"""Validate task archives before any upload or authentication request."""

from __future__ import annotations

import zipfile
from pathlib import Path

from ._transport import invalid

MAX_BYTES = 100 * 1024 * 1024
SHOTS_KEY = "TIANYAN_APP_SHOTS"


def validate_archive(package) -> None:
    path = Path(package)
    try:
        size = path.stat().st_size
        handle = open(path, "rb")
    except OSError:
        raise invalid("package must be a readable ZIP file") from None
    with handle:
        if size > MAX_BYTES:
            raise invalid("ZIP exceeds the 100 MB upload limit")
        try:
            archive = zipfile.ZipFile(handle)
        except (zipfile.BadZipFile, OSError):
            raise invalid("package is not a valid ZIP archive") from None
        with archive:
            _check_entries(archive)


def _check_entries(archive: zipfile.ZipFile) -> None:
    main_count = qcis_count = env_count = 0
    total = 0
    for info in archive.infolist():
        if info.is_dir():
            continue
        total += info.file_size
        if total > MAX_BYTES:
            raise invalid("uncompressed ZIP exceeds the 100 MB limit")
        if info.filename == "main.py":
            main_count += 1
        if info.filename.endswith(".qcis"):
            qcis_count += 1
        if info.filename == "tianyan.env":
            env_count += 1
            # Only the task configuration is read, never local credentials.
            try:
                with archive.open(info) as member:
                    content = member.read(MAX_BYTES + 1).decode("utf-8")
            except (UnicodeDecodeError, OSError, zipfile.BadZipFile, RuntimeError):
                raise invalid("tianyan.env must be valid UTF-8") from None
            if len(content.encode("utf-8")) > MAX_BYTES:
                raise invalid("tianyan.env exceeds the 100 MB limit")
            _validate_shots(content)
    if main_count != 1:
        raise invalid("ZIP root must contain exactly one main.py")
    if qcis_count != 1:
        raise invalid(f"ZIP must contain exactly one .qcis file (found {qcis_count})")
    if env_count > 1:
        raise invalid("ZIP contains duplicate tianyan.env entries")


def _validate_shots(content: str) -> None:
    values = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if key.strip() == SHOTS_KEY:
            values.append(value.strip())
    if len(values) != 1:
        raise invalid(f"tianyan.env must define {SHOTS_KEY} exactly once")
    value = values[0]
    if not (value.isascii() and value.isdigit()) or not 0 < int(value) <= 2**31 - 1:
        raise invalid(f"{SHOTS_KEY} must be a positive 32-bit integer")
