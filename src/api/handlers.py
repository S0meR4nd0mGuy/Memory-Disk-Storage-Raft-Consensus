"""Request handlers for API endpoints"""

from typing import Dict, Any
from src.logging_config import kv_logger

logger_base = kv_logger("kvstore_base", "log_file.log")
logger_api = kv_logger("kvstore_api", "api/api_log.log", format_style="full")


async def get_handler(request_data: Dict[str, Any], raft_node) -> Dict[str, Any]:
    """Handle GET request"""
    key = request_data.get("key")
    if not key:
        return {"error": "key required"}
    
    value = await raft_node.storage.get(key)
    return {"value": value}


async def put_handler(request_data: Dict[str, Any], raft_node) -> Dict[str, Any]:
    """Handle PUT request"""
    key = request_data.get("key")
    value = request_data.get("value")
    
    if not key or value is None:
        return {"error": "key and value required"}
    
    # Create command for Raft log
    command = {"op": "PUT", "key": key, "value": value}
    
    if hasattr(raft_node, "submit_command"):
        peers = getattr(raft_node, "peer_nodes", [])
        result = await raft_node.submit_command(command, peers)
        if result.get("status") != "ok":
            return result
    elif hasattr(raft_node, "storage"):
        await raft_node.storage.put(key, value)
    
    return {"status": "ok"}


async def delete_handler(request_data: Dict[str, Any], raft_node) -> Dict[str, Any]:
    """Handle DELETE request"""
    key = request_data.get("key")
    if not key:
        return {"error": "key required"}
    
    command = {"op": "DELETE", "key": key}
    
    if hasattr(raft_node, "submit_command"):
        peers = getattr(raft_node, "peer_nodes", [])
        result = await raft_node.submit_command(command, peers)
        if result.get("status") != "ok":
            return result
    elif hasattr(raft_node, "storage"):
        await raft_node.storage.delete(key)
    
    return {"status": "ok"}


async def scan_handler(request_data: Dict[str, Any], raft_node) -> Dict[str, Any]:
    """Handle range scan request"""
    start_key = request_data.get("start_key", "")
    end_key = request_data.get("end_key", "\xff")
    
    result = await raft_node.storage.scan(start_key, end_key)
    return {"data": result}
