"""Tests for API handlers"""

import pytest
from src.api.handlers import get_handler, put_handler, delete_handler
from src.consensus.raft import NodeState
from src.consensus.state_machine import StateMachine
from src.main import build_storage


@pytest.mark.asyncio
async def test_get_handler():
    """Test GET handler"""
    # Mock raft node
    class MockStorage:
        async def get(self, key):
            if key == "key1":
                return "value1"
            return None
    
    class MockRaftNode:
        def __init__(self):
            self.storage = MockStorage()
    
    raft_node = MockRaftNode()
    result = await get_handler({"key": "key1"}, raft_node)
    
    assert result["value"] == "value1"


@pytest.mark.asyncio
async def test_put_handler():
    """Test PUT handler"""
    class MockRaftNode:
        pass
    
    raft_node = MockRaftNode()
    result = await put_handler({"key": "key1", "value": "value1"}, raft_node)
    
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_delete_handler():
    """Test DELETE handler"""
    class MockRaftNode:
        pass
    
    raft_node = MockRaftNode()
    result = await delete_handler({"key": "key1"}, raft_node)
    
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_main_stack_commits_api_put_through_raft(tmp_path):
    """API writes should commit through Raft and apply to durable storage."""
    from config.default import Config
    from src.consensus.raft import RaftNode

    config = Config()
    config.STORAGE_DATA_DIR = str(tmp_path / "data")

    storage = build_storage(config)
    await storage.recover()

    raft_node = RaftNode("node1", [], StateMachine(storage))
    await raft_node.start_election([])

    assert raft_node.state == NodeState.LEADER

    result = await put_handler({"key": "key1", "value": "value1"}, raft_node)

    assert result["status"] == "ok"
    assert await storage.get("key1") == "value1"
