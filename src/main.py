"""Entry point for the KV store server."""

import asyncio

from src.config.default import Config
from src.api.server import APIServer
from src.consensus.raft import RaftNode
from src.consensus.state_machine import StateMachine
from src.network.rpc import RPC, register_raft_handlers
from src.storage.lsm import LSMTree
from src.storage.memory import InMemoryStorage
from src.storage.storage import PersistentStorage
from src.storage.wal import WriteAheadLog


def build_storage(config: Config) -> PersistentStorage:
    """Build the configured WAL-backed storage engine."""
    if config.STORAGE_TYPE == "lsm":
        engine = LSMTree()
    elif config.STORAGE_TYPE == "memory":
        engine = InMemoryStorage()
    else:
        raise ValueError(f"Unsupported STORAGE_TYPE: {config.STORAGE_TYPE}")

    wal = WriteAheadLog(log_dir=f"{config.STORAGE_DATA_DIR}/wal")
    return PersistentStorage(engine, wal)


async def main() -> None:
    config = Config.from_env()

    storage = build_storage(config)
    await storage.recover()

    state_machine = StateMachine(storage)
    raft_node = RaftNode(
        node_id=config.NODE_ID,
        peers=[],
        state_machine=state_machine,
        min_election_timeout_ms=config.RAFT_MIN_ELECTION_TIMEOUT_MS,
        max_election_timeout_ms=config.RAFT_MAX_ELECTION_TIMEOUT_MS,
    )

    rpc = RPC(node_id=config.NODE_ID)
    register_raft_handlers(rpc, raft_node)
    raft_node.rpc = rpc

    # A single-node cluster can safely elect itself and commit locally.
    await raft_node.start_election([])

    server = APIServer(config.API_HOST, config.API_PORT, raft_node=raft_node)
    print(
        f"Starting KV store node {config.NODE_ID} on "
        f"http://{config.API_HOST}:{config.API_PORT}"
    )
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
