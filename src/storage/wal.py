"""Write-Ahead Log for durability"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import os
from src.logging_config import kv_logger

logger_base = kv_logger("kvstore_base", "log_file.log")
logger_storage = kv_logger("kvstore_storage", "storage/storage_log.log", format_style="full")


class WriteAheadLog:
    """Write-Ahead Log for crash recovery"""

    def __init__(self, log_dir: str = "./data/wal"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.entries: List[Dict[str, Any]] = []
        logger_storage.debug(f"Initialized WAL at {log_dir}")

    async def append(self, entry: Dict[str, Any]) -> None:
        """Append entry to WAL"""
        entry["timestamp"] = datetime.now(timezone.utc).isoformat()
        self.entries.append(entry)
        
        # Write to disk immediately
        await self._flush()
        logger_storage.debug(f"WAL append: {entry}")

    async def truncate(self, up_to: int) -> None:
        """Truncate WAL up to index"""
        self.entries = self.entries[up_to:]
        await self._flush()
        logger_storage.info(f"WAL truncated at index {up_to}")

    async def _flush(self) -> None:
        """Flush WAL to disk"""
        log_file = self.log_dir / "entries.jsonl"
        with open(log_file, "w") as f:
            for entry in self.entries:
                f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())

    async def load(self) -> List[Dict[str, Any]]:
        """Load WAL from disk"""
        log_file = self.log_dir / "entries.jsonl"
        if not log_file.exists():
            return []
        
        entries = []
        with open(log_file, "r") as f:
            for line in f:
                if line.strip():
                    entries.append(json.loads(line))
        
        self.entries = entries
        logger_storage.info(f"Loaded {len(entries)} entries from WAL")
        return entries
