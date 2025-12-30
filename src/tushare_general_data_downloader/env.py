"""Lightweight .env loader for local runs."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def _env_paths_to_try() -> Iterable[Path]:
    """Yield plausible locations of a .env file for convenience."""
    cwd = Path.cwd()
    yield cwd / ".env"
    yield cwd / ".env.local"

    script_dir = Path(__file__).resolve().parent
    for parent in [script_dir, *script_dir.parents]:
        yield parent / ".env"
        yield parent / ".env.local"


def load_local_env() -> Path | None:
    """Populate environment variables from the first existing .env file."""
    for env_path in _env_paths_to_try():
        if not env_path.exists():
            continue

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
        return env_path
    return None
