"""LSM-Tree (Log-Structured Merge-Tree) storage implementation

This is a simplified version of what RocksDB/LevelDB use internally.
"""

from typing import Optional, Any, List
from .storage import StorageEngine
from src.logging_config import base_logger, kv_logger

logger_base = base_logger()
logger_storage = kv_logger("kvstore_storage", "storage/storage_log.log", format_style="full")


class LSMTree(StorageEngine):
    """
    LSM-Tree storage engine

    Combines multiple sorted runs (in-memory and on-disk) for efficient writes.
    Compaction merges levels to optimize read performance.

    NOTE: this implementation keeps "sstables" purely in memory (a list of
    dicts) — there is no actual disk persistence here, and no real merge
    compaction (older sstables are never merged/pruned). Durability is
    expected to be provided externally by wrapping this engine in
    PersistentStorage (WAL-backed). This class alone provides no crash
    safety.
    """

    def __init__(self, max_memtable_size: int = 1000):
        self.memtable: dict = {}
        self.sstables: List[dict] = []
        self.max_memtable_size = max_memtable_size
        logger_storage.debug(f"Initialized LSM-Tree with max_memtable_size={max_memtable_size}")

    async def get(self, key: str) -> Optional[Any]:
        """Retrieve value by key (checks memtable, then sstables newest-first)"""
        logger_storage.debug(f"Get key={key}")
        # Memtable is always the newest data
        if key in self.memtable:
            return self.memtable[key]

        # Check sstables in reverse order (newest first) — first match wins
        for sstable in reversed(self.sstables):
            if key in sstable:
                return sstable[key]

        return None

    async def put(self, key: str, value: Any) -> None:
        """Store key-value pair in memtable"""
        self.memtable[key] = value

        # Trigger compaction if memtable exceeds threshold
        if len(self.memtable) >= self.max_memtable_size:
            await self._compact()

        logger_storage.debug(f"Put key={key}, value={value}")

    async def delete(self, key: str) -> None:
        """Mark key as deleted (tombstone)"""
        self.memtable[key] = None
        logger_storage.debug(f"Deleted key={key}")
        logger_base.info("A key has been deleted.")

    async def scan(self, start_key: str, end_key: str) -> dict:
        """
        Range scan across all levels.

        Merge oldest -> newest so that later writes (including tombstones)
        correctly overwrite earlier ones for the same key, then strip
        tombstones only once, at the end.
        """
        logger_storage.debug(f"Scan range start={start_key}, end={end_key}")
        merged: dict = {}

        # Oldest sstables first
        for sstable in self.sstables:
            for k, v in sstable.items():
                if start_key <= k <= end_key:
                    merged[k] = v  # unconditional overwrite — do not filter yet

        # Memtable is newest, applied last so it wins over any sstable value
        for k, v in self.memtable.items():
            if start_key <= k <= end_key:
                merged[k] = v

        # Only now drop tombstones, after all layers have been reconciled
        return {k: v for k, v in merged.items() if v is not None}

    async def snapshot(self) -> dict:
        """
        Get complete snapshot.

        Same merge-then-filter approach as scan(), just without a key range.
        """
        logger_storage.debug("Creating LSM snapshot")
        merged: dict = {}

        for sstable in self.sstables:
            merged.update(sstable)  # unconditional overwrite, oldest -> newest

        merged.update(self.memtable)  # newest layer wins

        return {k: v for k, v in merged.items() if v is not None}

    async def restore(self, snapshot: dict) -> None:
        """Restore from snapshot"""
        self.memtable = snapshot.copy()
        self.sstables = []
        logger_base.info("Restored from snapshot")
        logger_storage.info("A snapshot has been restored.")

    async def _compact(self) -> None:
        """
        Flush memtable to a new sstable.

        NOTE: this only flushes — it does not merge/prune existing sstables,
        so self.sstables grows unbounded over time and get()/scan() get
        linearly slower. Real merge compaction (dedup across sstables,
        garbage-collecting tombstones once safe) is a known stretch goal,
        not implemented yet.
        """
        if self.memtable:
            self.sstables.append(self.memtable.copy())
            self.memtable = {}
            logger_storage.info(f"Compacted memtable, now {len(self.sstables)} sstables")
