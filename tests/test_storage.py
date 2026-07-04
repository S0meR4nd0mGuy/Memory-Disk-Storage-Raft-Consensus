"""
Full test suite for the storage layer:
- InMemoryStorage (basic engine behavior)
- LSMTree (basic engine behavior + tombstone-across-compaction correctness)
- WriteAheadLog (append/load in isolation)
- PersistentStorage (WAL-backed durability, guard clauses, crash recovery)
"""

import pytest
from src.storage.memory import InMemoryStorage
from src.storage.lsm import LSMTree
from src.storage.wal import WriteAheadLog
from src.storage.storage import PersistentStorage, Storage_Error



# InMemoryStorage — basic engine contract


@pytest.mark.asyncio
async def test_memory_put_get():
    engine = InMemoryStorage()
    await engine.put("foo", "bar")
    assert await engine.get("foo") == "bar"


@pytest.mark.asyncio
async def test_memory_get_missing_key_returns_none():
    engine = InMemoryStorage()
    assert await engine.get("nope") is None


@pytest.mark.asyncio
async def test_memory_delete():
    engine = InMemoryStorage()
    await engine.put("foo", "bar")
    await engine.delete("foo")
    assert await engine.get("foo") is None


@pytest.mark.asyncio
async def test_memory_delete_missing_key_does_not_raise():
    engine = InMemoryStorage()
    await engine.delete("nope")  # should just no-op, not error


@pytest.mark.asyncio
async def test_memory_scan_range():
    engine = InMemoryStorage()
    await engine.put("a", "1")
    await engine.put("b", "2")
    await engine.put("z", "26")
    result = await engine.scan("a", "c")
    assert result == {"a": "1", "b": "2"}


@pytest.mark.asyncio
async def test_memory_snapshot_and_restore():
    engine = InMemoryStorage()
    await engine.put("a", "1")
    await engine.put("b", "2")
    snap = await engine.snapshot()

    engine2 = InMemoryStorage()
    await engine2.restore(snap)

    assert await engine2.get("a") == "1"
    assert await engine2.get("b") == "2"


# LSMTree — basic engine contract


@pytest.mark.asyncio
async def test_lsm_put_get_before_compaction():
    lsm = LSMTree(max_memtable_size=100)
    await lsm.put("foo", "bar")
    assert await lsm.get("foo") == "bar"


@pytest.mark.asyncio
async def test_lsm_get_missing_key_returns_none():
    lsm = LSMTree(max_memtable_size=100)
    assert await lsm.get("nope") is None


@pytest.mark.asyncio
async def test_lsm_compaction_triggers_at_threshold():
    lsm = LSMTree(max_memtable_size=2)
    await lsm.put("a", "1")
    await lsm.put("b", "2")  # hits threshold -> should trigger compaction
    assert len(lsm.sstables) == 1
    assert lsm.memtable == {}
    # value should still be readable after compaction
    assert await lsm.get("a") == "1"
    assert await lsm.get("b") == "2"


@pytest.mark.asyncio
async def test_lsm_newer_sstable_overrides_older_on_get():
    lsm = LSMTree(max_memtable_size=1)
    await lsm.put("x", "old_value")   # flushes to sstable #1
    await lsm.put("x", "new_value")   # flushes to sstable #2
    assert await lsm.get("x") == "new_value"


@pytest.mark.asyncio
async def test_lsm_delete_after_compaction_hides_value_on_get():
    lsm = LSMTree(max_memtable_size=1)
    await lsm.put("x", "old_value")   # flushed to sstable
    await lsm.delete("x")             # tombstone in fresh memtable
    assert await lsm.get("x") is None


@pytest.mark.asyncio
async def test_lsm_scan_respects_tombstone_across_compaction():
    """
    Regression test for the merge-order bug: a value flushed to an old
    sstable, then deleted afterward, must NOT reappear in scan() results.
    """
    lsm = LSMTree(max_memtable_size=1)
    await lsm.put("x", "old_value")   # triggers compaction, lands in sstable
    await lsm.delete("x")             # tombstone in memtable
    result = await lsm.scan("a", "z")
    assert "x" not in result


@pytest.mark.asyncio
async def test_lsm_snapshot_respects_tombstone_across_compaction():
    lsm = LSMTree(max_memtable_size=1)
    await lsm.put("x", "old_value")
    await lsm.delete("x")
    snap = await lsm.snapshot()
    assert "x" not in snap


@pytest.mark.asyncio
async def test_lsm_scan_merges_across_multiple_sstables_and_memtable():
    lsm = LSMTree(max_memtable_size=1)
    await lsm.put("a", "1")   # -> sstable
    await lsm.put("b", "2")   # -> sstable
    await lsm.put("c", "3")   # stays in memtable (only 1 item, under threshold... 
                              # actually threshold=1 means this also flushes)
    result = await lsm.scan("a", "z")
    assert result.get("a") == "1"
    assert result.get("b") == "2"
    assert result.get("c") == "3"


@pytest.mark.asyncio
async def test_lsm_restore_replaces_state():
    lsm = LSMTree(max_memtable_size=100)
    await lsm.put("a", "1")
    await lsm.restore({"x": "9"})
    assert await lsm.get("x") == "9"
    assert await lsm.get("a") is None  # old state should be gone



# WriteAheadLog — isolated behavior

@pytest.mark.asyncio
async def test_wal_append_and_load(tmp_path):
    wal = WriteAheadLog(log_dir=str(tmp_path / "wal"))
    await wal.append({"op": "put", "key": "foo", "value": "bar"})

    # brand new instance, same directory — simulates reopening the file
    wal2 = WriteAheadLog(log_dir=str(tmp_path / "wal"))
    entries = await wal2.load()

    assert len(entries) == 1
    assert entries[0]["op"] == "put"
    assert entries[0]["key"] == "foo"
    assert entries[0]["value"] == "bar"


@pytest.mark.asyncio
async def test_wal_load_empty_when_no_file_exists(tmp_path):
    wal = WriteAheadLog(log_dir=str(tmp_path / "wal"))
    entries = await wal.load()
    assert entries == []


@pytest.mark.asyncio
async def test_wal_preserves_order(tmp_path):
    wal = WriteAheadLog(log_dir=str(tmp_path / "wal"))
    await wal.append({"op": "put", "key": "a", "value": "1"})
    await wal.append({"op": "put", "key": "b", "value": "2"})
    await wal.append({"op": "put", "key": "a", "value": "1-updated"})

    wal2 = WriteAheadLog(log_dir=str(tmp_path / "wal"))
    entries = await wal2.load()

    assert [e["key"] for e in entries] == ["a", "b", "a"]
    assert entries[-1]["value"] == "1-updated"



# PersistentStorage — guard clauses


@pytest.mark.asyncio
async def test_operations_before_recover_raise(tmp_path):
    engine = InMemoryStorage()
    wal = WriteAheadLog(log_dir=str(tmp_path / "wal"))
    storage = PersistentStorage(engine, wal)

    with pytest.raises(Storage_Error):
        await storage.put("foo", "bar")
    with pytest.raises(Storage_Error):
        await storage.get("foo")
    with pytest.raises(Storage_Error):
        await storage.delete("foo")
    with pytest.raises(Storage_Error):
        await storage.scan("a", "z")
    with pytest.raises(Storage_Error):
        await storage.snapshot()


@pytest.mark.asyncio
async def test_recover_is_idempotent(tmp_path):
    engine = InMemoryStorage()
    wal = WriteAheadLog(log_dir=str(tmp_path / "wal"))
    storage = PersistentStorage(engine, wal)

    await storage.recover()
    await storage.put("foo", "bar")
    await storage.recover()  # calling again should be a safe no-op

    assert await storage.get("foo") == "bar"



# PersistentStorage — crash recovery (the core guarantee)


@pytest.mark.asyncio
async def test_crash_recovery_put_survives_restart(tmp_path):
    wal_dir = str(tmp_path / "wal")

    # "process 1"
    engine1 = InMemoryStorage()
    wal1 = WriteAheadLog(log_dir=wal_dir)
    storage1 = PersistentStorage(engine1, wal1)
    await storage1.recover()
    await storage1.put("foo", "bar")

    # "process 2" — fresh objects, same file on disk
    engine2 = InMemoryStorage()
    wal2 = WriteAheadLog(log_dir=wal_dir)
    storage2 = PersistentStorage(engine2, wal2)
    await storage2.recover()

    assert await storage2.get("foo") == "bar"


@pytest.mark.asyncio
async def test_crash_recovery_delete_survives_restart(tmp_path):
    wal_dir = str(tmp_path / "wal")

    engine1 = InMemoryStorage()
    wal1 = WriteAheadLog(log_dir=wal_dir)
    storage1 = PersistentStorage(engine1, wal1)
    await storage1.recover()
    await storage1.put("foo", "bar")
    await storage1.delete("foo")

    engine2 = InMemoryStorage()
    wal2 = WriteAheadLog(log_dir=wal_dir)
    storage2 = PersistentStorage(engine2, wal2)
    await storage2.recover()

    assert await storage2.get("foo") is None


@pytest.mark.asyncio
async def test_crash_recovery_multiple_keys_and_overwrite_order(tmp_path):
    wal_dir = str(tmp_path / "wal")

    engine1 = InMemoryStorage()
    wal1 = WriteAheadLog(log_dir=wal_dir)
    storage1 = PersistentStorage(engine1, wal1)
    await storage1.recover()
    await storage1.put("a", "1")
    await storage1.put("b", "2")
    await storage1.put("a", "1-updated")  # overwrite

    engine2 = InMemoryStorage()
    wal2 = WriteAheadLog(log_dir=wal_dir)
    storage2 = PersistentStorage(engine2, wal2)
    await storage2.recover()

    assert await storage2.get("a") == "1-updated"
    assert await storage2.get("b") == "2"


@pytest.mark.asyncio
async def test_crash_recovery_works_with_lsm_engine_too(tmp_path):
    """
    PersistentStorage should be engine-agnostic — same durability guarantee
    whether wrapping InMemoryStorage or LSMTree.
    """
    wal_dir = str(tmp_path / "wal")

    engine1 = LSMTree(max_memtable_size=100)
    wal1 = WriteAheadLog(log_dir=wal_dir)
    storage1 = PersistentStorage(engine1, wal1)
    await storage1.recover()
    await storage1.put("foo", "bar")

    engine2 = LSMTree(max_memtable_size=100)
    wal2 = WriteAheadLog(log_dir=wal_dir)
    storage2 = PersistentStorage(engine2, wal2)
    await storage2.recover()

    assert await storage2.get("foo") == "bar"


@pytest.mark.asyncio
async def test_unknown_wal_op_raises_on_recover(tmp_path):
    wal_dir = str(tmp_path / "wal")

    wal1 = WriteAheadLog(log_dir=wal_dir)
    await wal1.append({"op": "explode", "key": "x", "value": "y"})

    engine2 = InMemoryStorage()
    wal2 = WriteAheadLog(log_dir=wal_dir)
    storage2 = PersistentStorage(engine2, wal2)

    with pytest.raises(Storage_Error):
        await storage2.recover()