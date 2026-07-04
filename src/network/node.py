"""Network node for inter-node communication"""

from typing import Dict
from src.logging_config import kv_logger

logger_base = kv_logger("kvstore_base", "log_file.log")
logger_network = kv_logger("kvstore_network", "network/network_log.log", format_style="full")


class NetworkNode:
    """
    Network node that handles communication with peer nodes
    
    Manages connections, message queues, and failure detection.
    """

    def __init__(self, node_id: str, listen_addr: str, listen_port: int):
        self.node_id = node_id
        self.listen_addr = listen_addr
        self.listen_port = listen_port
        self.peers: Dict[str, "PeerConnection"] = {}
        logger_network.info(f"Initialized network node {node_id} at {listen_addr}:{listen_port}")
        logger_base.info("Initialized network node")

    async def connect_to_peer(self, peer_id: str, peer_addr: str, peer_port: int) -> None:
        """Establish connection to peer"""
        logger_network.info(f"Connecting to peer {peer_id} at {peer_addr}:{peer_port}")
        logger_base.info("Connecting to peer")
        # TODO: Implement actual connection logic
        self.peers[peer_id] = PeerConnection(peer_id, peer_addr, peer_port)

    async def disconnect_from_peer(self, peer_id: str) -> None:
        """Disconnect from peer"""
        if peer_id in self.peers:
            del self.peers[peer_id]
            logger_network.warning(f"Disconnected from peer {peer_id}")
            logger_base.warning("Disconnected from peer")

    async def send_message(self, peer_id: str, message: dict) -> None:
        """Send message to peer"""
        if peer_id not in self.peers:
            logger_network.critical(f"Peer {peer_id} not connected")
            logger_base.critical("Peer not connected")
            return
        
        # TODO: Implement actual send logic
        logger_network.debug(f"Sending message to {peer_id}")

    async def broadcast_message(self, message: dict) -> None:
        """Broadcast message to all peers"""
        for peer_id in self.peers:
            await self.send_message(peer_id, message)


class PeerConnection:
    """Connection to a peer node"""

    def __init__(self, peer_id: str, addr: str, port: int):
        self.peer_id = peer_id
        self.addr = addr
        self.port = port
        self.connected = False
        self.last_heartbeat = None
        logger_network.debug(f"Created peer connection to {peer_id}")
        logger_base.info("Created peer connection")
