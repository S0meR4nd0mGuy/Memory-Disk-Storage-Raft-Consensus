"""Raft log management and persistence"""

from typing import List, Optional, Any
from dataclasses import dataclass
from src.logging_config import kv_logger

logger_base = kv_logger("kvstore_base", "log_file.log")
logger_consensus = kv_logger("kvstore_consensus", "consensus/consensus_log.log", format_style="full")


@dataclass
class RaftLogEntry:
    """Entry in the Raft log"""
    term: int
    index: int
    command: Any


class RaftLog:
    """
    Raft log with persistence
    
    Maintains log entries, term information, and supports safe log queries.
    """

    def __init__(self):
        self.entries: List[RaftLogEntry] = []
        logger_consensus.debug("Initialized Raft log")

    def append(self, term: int, command: Any) -> RaftLogEntry:
        """Append entry to log"""
        index = len(self.entries)
        entry = RaftLogEntry(term=term, index=index, command=command)
        self.entries.append(entry)
        logger_consensus.debug(f"Appended entry: term={term}, index={index}")
        return entry

    def get_entry(self, index: int) -> Optional[RaftLogEntry]:
        """Get entry at index"""
        if 0 <= index < len(self.entries):
            return self.entries[index]
        return None

    def get_term(self, index: int) -> int:
        """Get term at index (0 if index out of bounds)"""
        entry = self.get_entry(index)
        return entry.term if entry else 0

    def last_index(self) -> int:
        """Get index of last entry"""
        return len(self.entries) - 1 if self.entries else -1

    def last_term(self) -> int:
        """Get term of last entry"""
        return self.get_term(self.last_index())

    def slice(self, from_index: int, to_index: Optional[int] = None) -> List[RaftLogEntry]:
        """Get slice of log entries"""
        return self.entries[from_index:to_index]

    def truncate_suffix(self, index: int) -> None:
        """Remove entries from index onwards"""
        self.entries = self.entries[:index]
        logger_consensus.info(f"Truncated log to index {index}")

    def clear(self) -> None:
        """Clear all entries"""
        self.entries = []
        logger_consensus.info("Cleared log")
