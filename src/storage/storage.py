"""Abstract storage engine interface"""

from abc import ABC, abstractmethod
from typing import Optional, Any
from .wal import WriteAheadLog
from src.logging_config import kv_logger

logger_storage = kv_logger("kvstore_storage", "storage/storage_log.log", format_style="full")

class Storage_Error(Exception):
    """Base class for storage engine errors"""
    pass
class StorageEngine(ABC):
    """Abstract base class for storage engines"""

    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Retrieve value by key"""
        pass

    @abstractmethod
    async def put(self, key: str, value: Any) -> None:
        """Store key-value pair"""
        pass

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete key"""
        pass

    @abstractmethod
    async def scan(self, start_key: str, end_key: str) -> dict:
        """Range scan from start_key to end_key"""
        pass

    @abstractmethod
    async def snapshot(self) -> dict:
        """Get complete snapshot of data"""
        pass

    @abstractmethod
    async def restore(self, snapshot: dict) -> None:
        """Restore from snapshot"""
        pass

class PersistentStorage(StorageEngine):
    """
    Wraps a StorageEngine (InMemoryStorage or LSMTree) with WAL-backed durability.

    Write path: WAL first, then engine.
    Read path: straight to engine, no WAL involvement.
    Startup: call recover() once before serving any requests.
    """

    def __init__(self, engine: StorageEngine, wal: WriteAheadLog):
        self.engine = engine
        self.wal = wal
        self.recovered: bool = False  # Track whether recover() has been called
        self.restored = False
        # TODO: anything else you need to track? e.g. has recover() run yet?

    async def recover(self) -> None:
        """
        Rebuild engine state from WAL on startup.
        - Load entries via self.wal.load()
        - Replay each into self.engine DIRECTLY (not via self.put/self.delete,
          or you'll re-append everything back into the WAL)
        - Log how many entries were replayed
        """
        if self.recovered:
            return

        entries = await self.wal.load()
        for entry in entries:
            op = entry["op"]
            if op == "put":
                # go straight to engine, not self.put()
                await self.engine.put(entry["key"], entry["value"])
            elif op == "delete":
                await self.engine.delete(entry["key"])
            else:
                logger_storage.critical(f"Unknown operation in WAL entry: {op}")
                raise Storage_Error(f"Unknown operation in WAL entry: {op}")

        self.recovered = True

    async def get(self, key: str) -> Optional[Any]:
        """Pass straight through to engine — no WAL involved in reads."""
        if not self.recovered:
            raise Storage_Error("Cannot get before recover() has been called")
        
        return await self.engine.get(key)

    async def put(self, key: str, value: Any) -> None:
        """
        1. Append {"op": "put", "key": key, "value": value} to WAL, awaited
        2. Only then call self.engine.put(key, value)
        """
        append_entry = {"op": "put", "key": key, "value": value}
        if not self.recovered:
            raise Storage_Error("Cannot write before recover() has been called")
        await self.wal.append(append_entry)
        await self.engine.put(key, value)

    async def delete(self, key: str) -> None:
        """
        1. Append {"op": "delete", "key": key} to WAL
        2. Only then call self.engine.delete(key)
        """
        append_entry = {"op": "delete", "key": key}
        if not self.recovered:
            raise Storage_Error("Cannot delete before recover() has been called")

        await self.wal.append(append_entry)
        await self.engine.delete(key)

    async def scan(self, start_key: str, end_key: str) -> dict:
        """Pass straight through to engine."""
        if not self.recovered:
            raise Storage_Error("Cannot scan before recover() has been called")
        return await self.engine.scan(start_key, end_key)

    async def snapshot(self) -> dict:
        """
        Pass straight through to engine for now.
        (Later: this is where you'd also trigger a WAL truncate — not yet.)
        """
        if not self.recovered:
            raise Storage_Error("Cannot snapshot before recover() has been called")
        return await self.engine.snapshot()

    async def restore(self, snapshot: dict) -> None:
        """
        Pass straight through to engine.
        Think about: should this also clear/truncate the WAL? Not yet —
        just note the question for later.
        """
        # TODO: one-time-only assumption is likely too strict for Raft snapshot-catchup later
        if self.restored:
            raise Storage_Error("restore() has already been called")
        await self.engine.restore(snapshot)
        self.restored = True
