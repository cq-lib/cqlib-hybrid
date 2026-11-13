"""Shared configuration resolution for CLI and Python entrypoints."""

from __future__ import annotations

import os


def resolve_login_key(explicit: str | None = None) -> str | None:
    return explicit or next(
        (
            os.environ[key]
            for key in ("TIANYAN_API_KEY", "CQFUSION_LOGIN_KEY", "LOGIN_KEY")
            if os.environ.get(key)
        ),
        None,
    )
