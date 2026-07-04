"""Default configuration"""

import os
from typing import Optional


class Config:
    """Application configuration"""

    # Raft settings
    RAFT_MIN_ELECTION_TIMEOUT_MS = int(os.getenv("RAFT_MIN_ELECTION_TIMEOUT_MS", "150"))
    RAFT_MAX_ELECTION_TIMEOUT_MS = int(os.getenv("RAFT_MAX_ELECTION_TIMEOUT_MS", "300"))
    RAFT_HEARTBEAT_INTERVAL_MS = int(os.getenv("RAFT_HEARTBEAT_INTERVAL_MS", "50"))

    # Storage settings
    STORAGE_TYPE = os.getenv("STORAGE_TYPE", "memory")  # "memory", "lsm", "btree"
    STORAGE_DATA_DIR = os.getenv("STORAGE_DATA_DIR", "./data")

    # Network settings
    NODE_ID = os.getenv("NODE_ID", "node1")
    LISTEN_ADDR = os.getenv("LISTEN_ADDR", "0.0.0.0")
    LISTEN_PORT = int(os.getenv("LISTEN_PORT", "5000"))

    # API settings
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    @staticmethod
    def from_env() -> "Config":
        """Load config from environment"""
        return Config()
