"""Shared logging setup for the key-value store."""

from pathlib import Path

from advanced_logging import LogLevel, get_logger


LOG_ROOT = Path(__file__).resolve().parent
IMPORTANT_PREFIX = "IMPORTANT:"


def _important_only(record) -> bool:
    """Keep the root application log focused on operationally important events."""
    return record.level >= LogLevel.WARNING or record.message.startswith(IMPORTANT_PREFIX)


def kv_logger(name: str, relative_file: str, important_only: bool = False, **kwargs):
    """Create a logger that writes inside this repository."""
    log_file = LOG_ROOT / relative_file
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = get_logger(name, console=False, file=str(log_file), **kwargs)
    if important_only and not getattr(logger, "_kv_important_filter_added", False):
        logger.add_filter(_important_only)
        logger._kv_important_filter_added = True
    return logger


def base_logger():
    """Logger for high-signal process-wide events only."""
    return kv_logger("kvstore_base", "log_file.log", important_only=True)


def important(message: str) -> str:
    """Mark an INFO event as important enough for the base log."""
    return f"{IMPORTANT_PREFIX} {message}"
