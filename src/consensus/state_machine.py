"""State machine for applying committed log entries"""

from typing import Any, Dict, Optional
from src.logging_config import base_logger, kv_logger

logger_base = base_logger()
logger_consensus = kv_logger("kvstore_consensus", "consensus/consensus_log.log", format_style="full")


class StateMachine:
    """
    State machine that applies Raft log entries
    
    This is the "business logic" that processes commands from the Raft log.
    For a key-value store, it applies GET/PUT/DELETE operations.
    """

    def __init__(self, storage_engine):
        self.storage = storage_engine
        logger_base.info("Initialized state machine")
        logger_consensus.info("Initialized state machine")

    async def apply(self, command: Dict[str, Any]) -> Any:
        """Apply a command to the state machine"""
        op = command.get("op")
        
        if op == "PUT":
            key = command["key"]
            value = command["value"]
            await self.storage.put(key, value)
            logger_base.info(f"Applied PUT: key={key}")
            logger_consensus.debug(f"Applied PUT: key={key}, value={value}")
            return {"status": "ok"}
        
        elif op == "DELETE":
            key = command["key"]
            await self.storage.delete(key)
            logger_base.info(f"Applied DELETE: key={key}")
            logger_consensus.debug(f"Applied DELETE: key={key}")
            return {"status": "ok"}
        
        elif op == "GET":
            key = command["key"]
            value = await self.storage.get(key)
            logger_base.info(f"Applied GET: key={key}")
            logger_consensus.debug(f"Applied GET: key={key}")
            return {"value": value}
        
        else:
            logger_base.warning(f"Unknown state machine operation: {op}")
            logger_consensus.warning(f"Unknown operation: {op}")
            return {"error": "unknown operation"}

    async def snapshot(self) -> Dict[str, Any]:
        """Create a snapshot of state machine state"""
        data = await self.storage.snapshot()
        return {"data": data}

    async def restore(self, snapshot: Dict[str, Any]) -> None:
        """Restore state machine from snapshot"""
        data = snapshot.get("data", {})
        await self.storage.restore(data)
        logger_consensus.info("Restored state machine from snapshot")
