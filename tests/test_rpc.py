"""Tests for the in-process RPC transport."""

import pytest

from src.consensus.raft import LogEntry, RaftNode
from src.network.rpc import RPC, RPCRequest, register_raft_handlers


@pytest.mark.asyncio
async def test_rpc_dispatches_registered_peer_handler():
    node1_rpc = RPC(node_id="node1")
    node2_rpc = RPC(node_id="node2")

    async def ping(data):
        return {"pong": data["value"]}

    node2_rpc.register_handler("Ping", ping)
    node1_rpc.register_peer("node2", node2_rpc)

    response = await node1_rpc.send_request("node2", "Ping", {"value": "ok"})

    assert response.success is True
    assert response.data == {"pong": "ok"}


@pytest.mark.asyncio
async def test_rpc_raft_append_entries_handler_accepts_serialized_entries():
    raft_node = RaftNode("node2", ["node1"])
    node_rpc = RPC(node_id="node2")
    register_raft_handlers(node_rpc, raft_node)

    response = await node_rpc.handle_request(
        RPCRequest(
            rpc_type="AppendEntries",
            sender_id="node1",
            data={
                "leader_id": "node1",
                "term": 1,
                "prev_log_index": -1,
                "prev_log_term": 0,
                "entries": [
                    {"term": 1, "index": 0, "command": {"op": "PUT", "key": "a"}}
                ],
                "leader_commit": 0,
            },
        )
    )

    assert response.success is True
    assert response.data["success"] is True
    assert raft_node.log == [LogEntry(term=1, index=0, command={"op": "PUT", "key": "a"})]
