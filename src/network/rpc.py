"""RPC protocol definitions and handlers"""

import inspect
from typing import Any, Dict, Callable, Optional
from dataclasses import dataclass

from src.consensus.raft import LogEntry
from src.logging_config import kv_logger

logger_base = kv_logger("kvstore_base", "log_file.log")
logger_network = kv_logger("kvstore_network", "network/network_log.log", format_style="full")


@dataclass
class RPCRequest:
    """Base RPC request"""
    rpc_type: str
    sender_id: str
    data: Dict[str, Any]


@dataclass
class RPCResponse:
    """Base RPC response"""
    success: bool
    data: Dict[str, Any]


class RPC:
    """
    RPC protocol for inter-node communication
    
    Can be backed by gRPC, raw TCP sockets, or HTTP.
    """

    def __init__(self, node_id: str = "local"):
        self.node_id = node_id
        self.handlers: Dict[str, Callable] = {}
        self.peers: Dict[str, "RPC"] = {}
        logger_base.info("Initialized RPC protocol")

    def register_handler(self, rpc_type: str, handler: Callable) -> None:
        """Register handler for RPC type"""
        self.handlers[rpc_type] = handler
        logger_network.debug(f"Registered handler for {rpc_type}")

    def register_peer(self, peer_id: str, peer_rpc: "RPC") -> None:
        """Register an in-process RPC peer."""
        self.peers[peer_id] = peer_rpc
        logger_network.debug(f"Registered peer RPC for {peer_id}")

    async def send_request(
        self, target: str, rpc_type: str, data: Dict[str, Any]
    ) -> Optional[RPCResponse]:
        """Send RPC request to target node"""
        logger_network.debug(f"Sending {rpc_type} to {target}")
        peer = self.peers.get(target)
        if peer is None:
            logger_network.warning(f"No RPC peer registered for {target}")
            return RPCResponse(success=False, data={"error": "peer unavailable"})

        request = RPCRequest(rpc_type=rpc_type, sender_id=self.node_id, data=data)
        return await peer.handle_request(request)

    async def handle_request(self, request: RPCRequest) -> RPCResponse:
        """Handle incoming RPC request"""
        handler = self.handlers.get(request.rpc_type)
        
        if not handler:
            logger_network.warning(f"No handler for {request.rpc_type}")

            return RPCResponse(success=False, data={"error": "unknown rpc type"})
        
        try:
            result = handler(request.data)
            if inspect.isawaitable(result):
                result = await result
            return RPCResponse(success=True, data=result)
        except Exception as e:
            logger_network.error(f"Error handling {request.rpc_type}: {e}")
            return RPCResponse(success=False, data={"error": str(e)})


def register_raft_handlers(rpc: RPC, raft_node) -> None:
    """Register RequestVote and AppendEntries handlers for a RaftNode."""

    async def request_vote(data: Dict[str, Any]) -> Dict[str, Any]:
        term, vote_granted = await raft_node.on_request_vote(
            candidate_id=data["candidate_id"],
            term=data["term"],
            last_log_index=data["last_log_index"],
            last_log_term=data["last_log_term"],
        )
        return {"term": term, "vote_granted": vote_granted}

    async def append_entries(data: Dict[str, Any]) -> Dict[str, Any]:
        entries = [
            entry if isinstance(entry, LogEntry) else LogEntry(**entry)
            for entry in data.get("entries", [])
        ]
        success = await raft_node.on_append_entries(
            leader_id=data["leader_id"],
            term=data["term"],
            prev_log_index=data["prev_log_index"],
            prev_log_term=data["prev_log_term"],
            entries=entries,
            leader_commit=data["leader_commit"],
        )
        await raft_node.apply_committed_entries()
        return {"term": raft_node.current_term, "success": success}

    rpc.register_handler("RequestVote", request_vote)
    rpc.register_handler("AppendEntries", append_entries)
