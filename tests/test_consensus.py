"""
Tests for the Raft consensus layer (leader election + basic AppendEntries).

Organized into:
- Election: single candidate, majority/no-majority, stale candidates
- RequestVote safety rules: one vote per term, log up-to-date checks
- AppendEntries: heartbeats, term handling, log matching, commit index
- Known gaps: tests marked xfail where the current implementation
  doesn't yet handle a case (documents intent, doesn't silently pass)
"""

import pytest
from src.consensus.raft import RaftNode, NodeState, LogEntry


# ---------------------------------------------------------------------------
# Leader election — happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_candidate_wins_election_with_unanimous_peers():
    node1 = RaftNode("node1", ["node2", "node3"])
    node2 = RaftNode("node2", ["node1", "node3"])
    node3 = RaftNode("node3", ["node1", "node2"])

    await node1.start_election([node2, node3])

    assert node1.state == NodeState.LEADER
    assert node1.current_term == 1


@pytest.mark.asyncio
async def test_candidate_wins_with_bare_majority_not_unanimous():
    """
    3-node cluster: candidate + 1 yes-vote peer is enough (2 out of 3),
    even if a third peer would refuse.
    """
    node1 = RaftNode("node1", ["node2", "node3"])
    node2 = RaftNode("node2", ["node1", "node3"])
    node3 = RaftNode("node3", ["node1", "node2"])

    # Rig node3 to already have voted for someone else this term,
    # so it will refuse node1's vote request.
    node3.current_term = 1
    node3.voted_for = "someone_else"

    await node1.start_election([node2, node3])

    assert node1.state == NodeState.LEADER  # 2 votes (self + node2) >= majority of 3


@pytest.mark.asyncio
async def test_candidate_loses_without_majority():
    """
    5-node cluster, but 3 peers refuse (already voted elsewhere this term).
    Candidate should NOT become leader.
    """
    node1 = RaftNode("node1", ["p2", "p3", "p4", "p5"])
    peers = [RaftNode(f"p{i}", []) for i in range(2, 6)]

    # Force 3 of 4 peers to have already voted for someone else at term 1
    for p in peers[:3]:
        p.current_term = 1
        p.voted_for = "someone_else"

    await node1.start_election(peers)

    assert node1.state != NodeState.LEADER
    assert node1.state == NodeState.CANDIDATE  # stays candidate, doesn't crash/flip incorrectly


@pytest.mark.asyncio
async def test_single_node_cluster_wins_election_trivially():
    """Edge case: no peers at all — a lone node should win its own vote (1 >= 1)."""
    node1 = RaftNode("node1", [])
    await node1.start_election([])
    assert node1.state == NodeState.LEADER


# ---------------------------------------------------------------------------
# Leader election — stepping down / term safety
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_candidate_steps_down_if_peer_has_higher_term():
    node1 = RaftNode("node1", ["node2"])
    node2 = RaftNode("node2", ["node1"])
    node2.current_term = 5  # node2 already ahead

    await node1.start_election([node2])

    assert node1.state == NodeState.FOLLOWER
    assert node1.current_term == 5


@pytest.mark.asyncio
async def test_candidate_does_not_win_if_it_becomes_follower_mid_election():
    """
    If a higher-term peer causes a step-down partway through the loop,
    the candidate must not later flip to LEADER even if remaining peers
    (not reached, since we return early) would have said yes.
    """
    node1 = RaftNode("node1", ["node2", "node3"])
    node2 = RaftNode("node2", ["node1", "node3"])
    node3 = RaftNode("node3", ["node1", "node2"])
    node2.current_term = 10  # will cause step-down on first peer contacted

    await node1.start_election([node2, node3])

    assert node1.state == NodeState.FOLLOWER
    assert node1.current_term == 10


# ---------------------------------------------------------------------------
# RequestVote — safety rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_voter_grants_at_most_one_vote_per_term():
    voter = RaftNode("voter", [])

    term_a, vote_a = await voter.on_request_vote("candidateA", 1, -1, 0)
    term_b, vote_b = await voter.on_request_vote("candidateB", 1, -1, 0)

    assert vote_a is True
    assert vote_b is False  # already committed to candidateA this term


@pytest.mark.asyncio
async def test_voter_can_vote_again_in_a_new_term():
    voter = RaftNode("voter", [])

    await voter.on_request_vote("candidateA", 1, -1, 0)
    term, vote = await voter.on_request_vote("candidateB", 2, -1, 0)

    assert vote is True  # new term resets voted_for


@pytest.mark.asyncio
async def test_voter_refuses_candidate_with_stale_term():
    voter = RaftNode("voter", [])
    voter.current_term = 5

    term, vote = await voter.on_request_vote("candidateA", 3, -1, 0)

    assert vote is False
    assert term == 5  # voter reports its real (higher) term back


@pytest.mark.asyncio
async def test_voter_refuses_candidate_with_older_log_term():
    """
    Candidate's last log term is lower than voter's — log is less
    up-to-date, must be refused even at the same raft term.
    """
    voter = RaftNode("voter", [])
    voter.current_term = 2
    voter.log = [LogEntry(term=2, index=0, command={"op": "PUT"})]

    # candidate claims last_log_term=1, which is older than voter's log
    term, vote = await voter.on_request_vote("candidateA", 2, 0, last_log_term=1)

    assert vote is False


@pytest.mark.asyncio
async def test_voter_grants_candidate_with_newer_log_term_even_if_shorter():
    """
    Candidate's last log TERM is higher, even though its log is
    shorter (lower index) — Raft prioritizes term over length.
    """
    voter = RaftNode("voter", [])
    voter.current_term = 3
    voter.log = [
        LogEntry(term=1, index=0, command={}),
        LogEntry(term=1, index=1, command={}),
        LogEntry(term=1, index=2, command={}),
    ]  # voter's last_log_term = 1, last_log_index = 2

    # candidate has fewer entries but a higher last_log_term
    term, vote = await voter.on_request_vote("candidateA", 3, last_log_index=0, last_log_term=2)

    assert vote is True


@pytest.mark.asyncio
async def test_voter_refuses_candidate_with_shorter_log_at_same_term():
    voter = RaftNode("voter", [])
    voter.current_term = 3
    voter.log = [
        LogEntry(term=1, index=0, command={}),
        LogEntry(term=1, index=1, command={}),
    ]  # last_log_index = 1, last_log_term = 1

    # candidate has same last_log_term but a shorter log
    term, vote = await voter.on_request_vote("candidateA", 3, last_log_index=0, last_log_term=1)

    assert vote is False


# ---------------------------------------------------------------------------
# AppendEntries — heartbeats / term handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_append_entries_rejects_stale_term():
    node = RaftNode("node", [])
    node.current_term = 5

    result = await node.on_append_entries(
        leader_id="oldleader", term=3, prev_log_index=-1,
        prev_log_term=0, entries=[], leader_commit=0
    )

    assert result is False


@pytest.mark.asyncio
async def test_append_entries_updates_heartbeat_timestamp():
    node = RaftNode("node", [])
    old_heartbeat = node.last_heartbeat

    await node.on_append_entries(
        leader_id="leader", term=1, prev_log_index=-1,
        prev_log_term=0, entries=[], leader_commit=0
    )

    assert node.last_heartbeat > old_heartbeat


@pytest.mark.asyncio
async def test_append_entries_converts_candidate_to_follower():
    """
    A CANDIDATE that receives valid AppendEntries from a legitimate
    leader (same or higher term) must step down to FOLLOWER.
    """
    node = RaftNode("node", [])
    node.state = NodeState.CANDIDATE
    node.current_term = 1

    await node.on_append_entries(
        leader_id="leader", term=1, prev_log_index=-1,
        prev_log_term=0, entries=[], leader_commit=0
    )

    # NOTE: current on_append_entries only demotes to FOLLOWER when
    # term > self.current_term, not when term == self.current_term.
    # This test documents that gap.
    assert node.state == NodeState.FOLLOWER


@pytest.mark.asyncio
async def test_append_entries_rejects_on_log_mismatch():
    node = RaftNode("node", [])
    node.log = [LogEntry(term=1, index=0, command={})]

    # claims prev_log_term=99 at index 0, but node's actual entry has term=1
    result = await node.on_append_entries(
        leader_id="leader", term=1, prev_log_index=0,
        prev_log_term=99, entries=[], leader_commit=0
    )

    assert result is False


@pytest.mark.asyncio
async def test_append_entries_appends_new_entries():
    node = RaftNode("node", [])
    new_entries = [LogEntry(term=1, index=0, command={"op": "PUT", "key": "a"})]

    result = await node.on_append_entries(
        leader_id="leader", term=1, prev_log_index=-1,
        prev_log_term=0, entries=new_entries, leader_commit=0
    )

    assert result is True
    assert len(node.log) == 1
    assert node.log[0].command == {"op": "PUT", "key": "a"}


@pytest.mark.asyncio
async def test_append_entries_advances_commit_index():
    node = RaftNode("node", [])
    node.log = [
        LogEntry(term=1, index=0, command={}),
        LogEntry(term=1, index=1, command={}),
    ]

    await node.on_append_entries(
        leader_id="leader", term=1, prev_log_index=1,
        prev_log_term=1, entries=[], leader_commit=5
    )

    # commit_index should be capped at len(log) - 1, not the raw leader_commit
    assert node.commit_index == 1


# ---------------------------------------------------------------------------
# apply_committed_entries
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_committed_entries_returns_only_newly_committed():
    node = RaftNode("node", [])
    node.log = [
        LogEntry(term=1, index=0, command={"op": "PUT", "key": "a"}),
        LogEntry(term=1, index=1, command={"op": "PUT", "key": "b"}),
    ]
    node.commit_index = 1
    node.last_applied = -1
    results = await node.apply_committed_entries()

    assert results == [
        {"op": "PUT", "key": "a"},
        {"op": "PUT", "key": "b"},
    ]
    assert node.last_applied == 1


@pytest.mark.asyncio
async def test_apply_committed_entries_is_idempotent_when_nothing_new():
    node = RaftNode("node", [])
    node.commit_index = 0
    node.last_applied = 0

    results = await node.apply_committed_entries()

    assert results == []