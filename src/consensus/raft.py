"""Raft consensus algorithm implementation

Based on the Raft paper (https://raft.github.io/)
"""

import random
from enum import Enum
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timezone

from src.logging_config import base_logger, important, kv_logger

logger_base = base_logger()
logger_consensus = kv_logger("kvstore_consensus", "consensus/consensus_log.log", format_style="full")


class NodeState(Enum):
    """Possible states for a Raft node"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """A single entry in the Raft log"""
    term: int
    index: int
    command: Any


class RaftNode:
    """
    Raft consensus node
    
    Implements leader election, log replication, and safety guarantees.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        state_machine=None,
        min_election_timeout_ms: int = 150,
        max_election_timeout_ms: int = 300,
    ):
        self.node_id = node_id
        self.peers = peers
        self.peer_nodes: List["RaftNode"] = []
        self.state_machine = state_machine
        self.storage = getattr(state_machine, "storage", None)
        self.min_election_timeout = min_election_timeout_ms / 1000
        self.max_election_timeout = max_election_timeout_ms / 1000
        
        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        # Volatile state
        self.state = NodeState.FOLLOWER
        self.commit_index = -1
        self.last_applied = -1
        
        # Leader state
        self.next_index: Dict[str, int] = {peer: 0 for peer in peers}
        self.match_index: Dict[str, int] = {peer: -1 for peer in peers}
        
        # Timing
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat = datetime.now(timezone.utc)
        
        logger_consensus.debug(f"Initialized Raft node {node_id} with peers {peers}")

    def _random_election_timeout(self) -> float:
        """Generate random election timeout"""
        return random.uniform(self.min_election_timeout, self.max_election_timeout)

    async def start_election(self, peers: List["RaftNode"]) -> None:
        """
        Start leader election.
        
        peers: the actual RaftNode instances to request votes from
            (in-process, no networking yet).
        """
        self.current_term += 1
        election_term = self.current_term  # snapshot
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        logger_consensus.info(f"Node {self.node_id} starting election for term {self.current_term}")
        last_log_index = len(self.log) - 1 if self.log else -1
        last_log_term = self.log[-1].term if self.log else 0
        threshold = ((len(peers)+1) // 2) + 1
        votes_received = 1  # vote for self
        
        for peer in peers:
            peer_term, vote = await peer.on_request_vote(
                self.node_id, self.current_term, last_log_index, last_log_term
            )
            if peer_term > self.current_term:
                self.current_term = peer_term
                self.state = NodeState.FOLLOWER
                return
            if vote:
                votes_received += 1

        if (
            votes_received >= threshold
            and self.state == NodeState.CANDIDATE
            and self.current_term == election_term  # nothing changed our term mid-election
        ):
            self.state = NodeState.LEADER
            logger_base.info(important(f"Node {self.node_id} became leader for term {self.current_term}"))
            logger_consensus.info(f"Node {self.node_id} became leader for term {self.current_term}")
            next_index = len(self.log)
            self.next_index = {peer.node_id: next_index for peer in peers}
            self.match_index = {peer.node_id: -1 for peer in peers}

    async def on_request_vote(self, candidate_id, term, last_log_index, last_log_term) -> tuple[int, bool]:
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None

        if term < self.current_term:
            logger_consensus.debug(
                f"Rejected vote for {candidate_id}: stale term {term}, current term {self.current_term}"
            )
            return self.current_term, False

        my_last_term = self.log[-1].term if self.log else 0
        my_last_index = len(self.log) - 1

        log_ok = (last_log_term > my_last_term) or (
            last_log_term == my_last_term and last_log_index >= my_last_index
        )

        if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            self.voted_for = candidate_id
            logger_consensus.debug(f"Granted vote to {candidate_id} for term {self.current_term}")
            return self.current_term, True

        logger_consensus.debug(f"Rejected vote for {candidate_id} for term {term}")
        return self.current_term, False

    async def on_append_entries(
        self, leader_id: str, term: int, prev_log_index: int, prev_log_term: int,
        entries: List[LogEntry], leader_commit: int
    ) -> bool:
        """Handle AppendEntries RPC"""
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
        elif term == self.current_term and self.state == NodeState.CANDIDATE:
            self.state = NodeState.FOLLOWER

        if term < self.current_term:
            logger_consensus.debug(
                f"Rejected AppendEntries from {leader_id}: stale term {term}, current term {self.current_term}"
            )
            return False
        
        self.last_heartbeat = datetime.now(timezone.utc)
        
        # Check if log matches at prev_log_index.
        if prev_log_index >= len(self.log):
            logger_consensus.debug(
                f"Rejected AppendEntries from {leader_id}: missing prev_log_index {prev_log_index}"
            )
            return False
        if prev_log_index >= 0 and self.log[prev_log_index].term != prev_log_term:
            logger_consensus.debug(
                f"Rejected AppendEntries from {leader_id}: term mismatch at index {prev_log_index}"
            )
            return False
        
        appended = 0
        for offset, entry in enumerate(entries):
            log_index = prev_log_index + 1 + offset
            normalized_entry = LogEntry(
                term=entry.term,
                index=log_index,
                command=entry.command,
            )

            if log_index < len(self.log):
                if self.log[log_index].term != entry.term:
                    self.log = self.log[:log_index]
                    self.log.append(normalized_entry)
                    appended += 1
                continue

            self.log.append(normalized_entry)
            appended += 1

        if appended:
            logger_consensus.debug(f"Node {self.node_id} appended {appended} entries")
        
        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            logger_consensus.debug(f"Node {self.node_id} advanced commit index to {self.commit_index}")
        
        return True

    async def apply_committed_entries(self) -> List[Any]:
        results = []
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            if self.state_machine is None:
                results.append(entry.command)
            else:
                results.append(await self.state_machine.apply(entry.command))
        return results

    async def submit_command(
        self,
        command: Any,
        peers: Optional[List["RaftNode"]] = None,
    ) -> Dict[str, Any]:
        """
        Append a client command on the leader and replicate it to in-process peers.

        Networking is not wired in yet, so callers can pass actual RaftNode
        instances for local tests or single-process demos.
        """
        if self.state != NodeState.LEADER:
            logger_consensus.warning(f"Node {self.node_id} rejected client command because it is not leader")
            return {
                "status": "error",
                "error": "not leader",
                "leader_id": None,
                "term": self.current_term,
            }

        peers = peers or []
        entry = LogEntry(term=self.current_term, index=len(self.log), command=command)
        self.log.append(entry)

        total_nodes = len(peers) + 1
        majority = (total_nodes // 2) + 1
        successful = 1

        for peer in peers:
            next_index = self.next_index.get(peer.node_id, len(self.log) - 1)
            while next_index >= 0:
                prev_log_index = next_index - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
                entries = self.log[next_index:]

                ok = await peer.on_append_entries(
                    leader_id=self.node_id,
                    term=self.current_term,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index,
                )
                if ok:
                    self.next_index[peer.node_id] = len(self.log)
                    self.match_index[peer.node_id] = len(self.log) - 1
                    successful += 1
                    break

                if peer.current_term > self.current_term:
                    self.current_term = peer.current_term
                    self.state = NodeState.FOLLOWER
                    return {
                        "status": "error",
                        "error": "stepped down",
                        "term": self.current_term,
                    }

                next_index -= 1
                self.next_index[peer.node_id] = next_index

        if successful >= majority:
            self.commit_index = entry.index
            logger_consensus.info(
                f"Committed log index {entry.index} in term {entry.term} with {successful}/{total_nodes} acks"
            )
            for peer in peers:
                await peer.on_append_entries(
                    leader_id=self.node_id,
                    term=self.current_term,
                    prev_log_index=len(self.log) - 1,
                    prev_log_term=self.log[-1].term if self.log else 0,
                    entries=[],
                    leader_commit=self.commit_index,
                )
                await peer.apply_committed_entries()
            await self.apply_committed_entries()
            return {"status": "ok", "index": entry.index, "term": entry.term}

        return {
            "status": "error",
            "error": "failed to reach majority",
            "acks": successful,
            "required": majority,
        }

    def is_leader(self) -> bool:
        """Check if this node is the leader"""
        return self.state == NodeState.LEADER
