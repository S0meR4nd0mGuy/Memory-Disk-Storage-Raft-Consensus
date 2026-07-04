"""Shared logging setup for the key-value store."""

from pathlib import Path

from advanced_logging import get_logger


LOG_ROOT = Path(__file__).resolve().parent


def kv_logger(name: str, relative_file: str, **kwargs):
    """Create a logger that writes inside this repository."""
    log_file = LOG_ROOT / relative_file
    log_file.parent.mkdir(parents=True, exist_ok=True)
    return get_logger(name, console=False, file=str(log_file), **kwargs)
