"""进程中断与恢复：批次全有或全无，恢复幂等，无遗留内容文件。"""
import os

import pytest

from cad_inbox import BatchItemRequest, CadInbox, IdempotencyConflictError
from cad_inbox.inbox import IDEMPOTENCY_TTL_SECONDS

from conftest import FakeClock, content_dirs, make_source, read_all


class SimulatedCrash(BaseException):
    """模拟进程被终止：不做任何清理直接抛出。"""


def crash_at(point):
    fired = {"hit": False}

    def hook(name):
        if name == point and not fired["hit"]:
            fired["hit"] = True
            raise SimulatedCrash(point)

    return hook


def attempt_submit(lib_dir, clock, point, reqs, key="k1"):
    """以故障注入方式提交，返回是否崩溃。"""
    box = CadInbox(lib_dir, now=clock, crash_hook=crash_at(point))
    try:
        box.submit_batch("p1", "s1", key, reqs)
        return False
    except SimulatedCrash:
        return True
    finally:
        box.close()


# 收件流程中所有可能的中断点
INTAKE_CRASH_POINTS = [
    "after_staging_rows",   # 仅登记了 staging 行
    "after_stage_file",     # 部分文件已复制
    "after_stage_files",    # 内容已全部保存，尚未移动
    "after_move_files",     # 内容已就位，回执尚未提交
    "after_commit",         # 已提交，但返回前崩溃
]


@pytest.mark.parametrize("point", INTAKE_CRASH_POINTS)
def test_crash_during_intake_all_or_nothing(lib_dir, clock, tmp_path, point):
    data = [os.urandom(512), os.urandom(768)]
    reqs = [
        BatchItemRequest(f"f{i}.dwg", make_source(tmp_path, f"f{i}.dwg", d))
        for i, d in enumerate(data)
    ]
    assert attempt_submit(lib_dir, clock, point, reqs) is True

    box = CadInbox(lib_dir, now=clock)
    box.recover()
    entries = box.list_session_files("p1", "s1")
    # 全有或全无
    assert len(entries) in (0, 2)
    if len(entries) == 2:  # after_commit：已提交批次必须完整保留
        for entry, original in zip(entries, data):
            with box.open_file(entry.file_id) as fh:
                assert fh.read() == original
    # 无遗留内容文件
    assert content_dirs(lib_dir) == ([], sorted({e.batch_id for e in entries}))
    # 重复恢复不改变结果
    box.recover()
    assert box.list_session_files("p1", "s1") == entries
    box.close()


@pytest.mark.parametrize("point", INTAKE_CRASH_POINTS)
def test_retry_after_crash_converges(lib_dir, clock, tmp_path, point):
    """崩溃恢复后重试：未提交的完整重交，已提交的命中同一回执。"""
    data = os.urandom(1024)
    src = make_source(tmp_path, "a.dwg", data)
    reqs = [BatchItemRequest("a.dwg", src)]
    attempt_submit(lib_dir, clock, point, reqs)

    box = CadInbox(lib_dir, now=clock)
    box.recover()
    receipt = box.submit_batch("p1", "s1", "k1", reqs)
    entries = box.list_session_files("p1", "s1")
    assert len(entries) == 1  # 恰好一份，不多不少
    with box.open_file(receipt.items[0].file_id) as fh:
        assert fh.read() == data
    # 再次重试仍命中同一回执
    assert box.submit_batch("p1", "s1", "k1", reqs) == receipt
    assert content_dirs(lib_dir) == ([], [receipt.batch_id])
    box.close()


def test_crash_after_commit_retry_hits_same_batch(lib_dir, clock, tmp_path):
    """提交后、返回前崩溃：原键重试必须命中同一批次。"""
    src = make_source(tmp_path, "a.dwg", b"committed-data")
    reqs = [BatchItemRequest("a.dwg", src)]
    attempt_submit(lib_dir, clock, "after_commit", reqs)

    box = CadInbox(lib_dir, now=clock)
    box.recover()
    r1 = box.submit_batch("p1", "s1", "k1", reqs)
    first_at = r1.submitted_at
    clock.advance(3600)
    r2 = box.submit_batch("p1", "s1", "k1", reqs)
    assert r2 == r1 and r2.submitted_at == first_at
    box.close()


def test_crash_during_recovery(lib_dir, clock, tmp_path):
    """恢复过程中再次中断：重跑恢复后状态正确，旧批次不受影响。"""
    old_data = b"old batch data"
    old_src = make_source(tmp_path, "old.dwg", old_data)
    box = CadInbox(lib_dir, now=clock)
    old_receipt = box.submit_batch("p1", "s1", "old-key",
                                   [BatchItemRequest("old.dwg", old_src)])
    box.close()

    # 制造一个未完成的批次
    new_src = make_source(tmp_path, "new.dwg", b"new data")
    attempt_submit(lib_dir, clock, "after_move_files",
                   [BatchItemRequest("new.dwg", new_src)], key="new-key")

    # 第一次恢复到一半就中断
    box = CadInbox(lib_dir, now=clock, crash_hook=crash_at("recover_after_remove_dir"))
    with pytest.raises(SimulatedCrash):
        box.recover()
    box.close()

    # 再次恢复：完整完成
    box = CadInbox(lib_dir, now=clock)
    box.recover()
    entries = box.list_session_files("p1", "s1")
    assert [e.file_id for e in entries] == [old_receipt.items[0].file_id]
    with box.open_file(old_receipt.items[0].file_id) as fh:
        assert fh.read() == old_data  # 旧批次逐字节一致
    # 没有遗留内容文件：只剩旧批次目录
    assert content_dirs(lib_dir) == ([], [old_receipt.batch_id])
    # 恢复幂等
    box.recover()
    assert content_dirs(lib_dir) == ([], [old_receipt.batch_id])
    box.close()


def test_recovery_does_not_need_sources(lib_dir, clock, tmp_path):
    """恢复不依赖原始源文件。"""
    src = make_source(tmp_path, "a.dwg", b"data")
    reqs = [BatchItemRequest("a.dwg", src)]
    attempt_submit(lib_dir, clock, "after_stage_files", reqs)
    os.remove(src)  # 源文件已不存在

    box = CadInbox(lib_dir, now=clock)
    box.recover()  # 回滚不读源文件
    assert box.list_session_files("p1", "s1") == []
    assert content_dirs(lib_dir) == ([], [])
    box.close()


def test_expired_key_reuse_interrupted(lib_dir, clock, tmp_path):
    """过期键复用过程中中断：旧批次始终可读，恢复后复用可完成。"""
    old_src = make_source(tmp_path, "a.dwg", b"old-content")
    box = CadInbox(lib_dir, now=clock)
    old_receipt = box.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", old_src)])
    box.close()

    clock.advance(IDEMPOTENCY_TTL_SECONDS)  # 恰好满 24h，键可复用
    new_src = make_source(tmp_path, "b.dwg", b"new-content")
    reqs = [BatchItemRequest("b.dwg", new_src)]
    # 复用键的新批次在回执提交前中断
    attempt_submit(lib_dir, clock, "after_move_files", reqs)

    box = CadInbox(lib_dir, now=clock)
    box.recover()
    # 旧批次完整可读
    with box.open_file(old_receipt.items[0].file_id) as fh:
        assert fh.read() == b"old-content"
    # 键已过期不等于文件过期：旧批次内容目录必须保留
    assert old_receipt.batch_id in content_dirs(lib_dir)[1]
    # 复用可重新完成，且不影响旧批次
    new_receipt = box.submit_batch("p1", "s1", "k1", reqs)
    assert new_receipt.batch_id != old_receipt.batch_id
    assert len(box.list_session_files("p1", "s1")) == 2
    box.close()


def test_changed_request_never_pollutes_live_batch(lib_dir, clock, tmp_path):
    """有效期内变更请求被拒绝，即使中途崩溃也不污染原批次。"""
    src_a = make_source(tmp_path, "a.dwg", b"content-a")
    box = CadInbox(lib_dir, now=clock)
    r1 = box.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", src_a)])
    box.close()

    src_b = make_source(tmp_path, "b.dwg", b"content-b")
    changed = [BatchItemRequest("b.dwg", src_b)]
    # 变更请求：拒绝
    box = CadInbox(lib_dir, now=clock)
    with pytest.raises(IdempotencyConflictError):
        box.submit_batch("p1", "s1", "k1", changed)
    box.close()
    # 变更请求在落盘阶段崩溃（staging 已登记但哈希检查前先算内容）
    attempt_submit(lib_dir, clock, "after_staging_rows", changed, key="k-other")

    box = CadInbox(lib_dir, now=clock)
    box.recover()
    entries = box.list_session_files("p1", "s1")
    assert len(entries) == 1
    assert entries[0].file_id == r1.items[0].file_id
    with box.open_file(r1.items[0].file_id) as fh:
        assert fh.read() == b"content-a"
    assert content_dirs(lib_dir) == ([], [r1.batch_id])
    box.close()


def test_orphan_dirs_cleaned_without_db_rows(lib_dir, clock, tmp_path):
    """库目录里出现无对应已提交批次的内容目录时，恢复会清除它们。"""
    src = make_source(tmp_path, "a.dwg", b"keep me")
    box = CadInbox(lib_dir, now=clock)
    receipt = box.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", src)])
    box.close()

    # 模拟崩溃残留的孤儿目录（DB 中无记录）
    os.makedirs(os.path.join(lib_dir, "staging", "deadbeef"))
    orphan = os.path.join(lib_dir, "files", "cafef00d")
    os.makedirs(orphan)
    with open(os.path.join(orphan, "000000"), "wb") as fh:
        fh.write(b"orphan")

    box = CadInbox(lib_dir, now=clock)
    box.recover()
    assert content_dirs(lib_dir) == ([], [receipt.batch_id])
    with box.open_file(receipt.items[0].file_id) as fh:
        assert fh.read() == b"keep me"
    box.close()
