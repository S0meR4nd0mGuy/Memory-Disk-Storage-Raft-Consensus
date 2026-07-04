"""In-memory storage implementation"""

from typing import Optional, Any
from .storage import StorageEngine
from src.logging_config import kv_logger

logger_base = kv_logger("kvstore_base", "log_file.log")
logger_storage = kv_logger("kvstore_storage", "storage/storage_log.log", format_style="full")


class InMemoryStorage(StorageEngine):
    """Simple in-memory hash map storage"""

    def __init__(self):
        self.data: dict = {}
        logger_storage.debug("Initialized in-memory storage")

    async def get(self, key: str) -> Optional[Any]:
        """Retrieve value by key"""
        return self.data.get(key)

    async def put(self, key: str, value: Any) -> None:
        """Store key-value pair"""
        self.data[key] = value
        logger_storage.debug(f"Put key={key}, value={value}")

    async def delete(self, key: str) -> None:
        """Delete key"""
        if key in self.data:
            del self.data[key]
            logger_storage.debug(f"Deleted key={key}")

    async def scan(self, start_key: str, end_key: str) -> dict:
        """Range scan from start_key to end_key"""
        result = {k: v for k, v in self.data.items() if start_key <= k <= end_key}
        return result

    async def snapshot(self) -> dict:
        """Get complete snapshot of data"""
        return self.data.copy()

    async def restore(self, snapshot: dict) -> None:
        """Restore from snapshot"""
        self.data = snapshot.copy()
        logger_base.info("Restored from snapshot")
        logger_storage.info("A snapshot has been restored.")
