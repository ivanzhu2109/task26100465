"""进程在收件或恢复任意时刻被终止后的恢复测试。"""

import os

import pytest

import cad_inbox.library as lib
from cad_inbox import CadInbox
from cad_inbox.library import IDEMPOTENCY_TTL

from conftest import make_items, read_all


class SimulatedCrash(BaseException):
    """模拟进程被终止：不被 ingest 的常规异常清理捕获。"""


def content_dir(root):
    return os.path.join(root, "content")


def staging_dir(root):
    return os.path.join(root, "staging")


def listdir(path):
    return os.listdir(path) if os.path.isdir(path) else []


def test_crash_during_staging_copy_rolls_back(inbox, src_dir, root, monkeypatch):
    """收件未完成：恢复后整批撤回，不留下半批文件。"""
    real_copy = CadInbox._copy_to_staging
    calls = {"n": 0}

    def boom(source_path, staging, seq):
        calls["n"] += 1
        if calls["n"] == 2:
            raise SimulatedCrash()
        return real_copy(source_path, staging, seq)

    monkeypatch.setattr(CadInbox, "_copy_to_staging", staticmethod(boom))
    items = make_items(src_dir, [("a.dwg", b"1"), ("b.dwg", b"2"), ("c.dwg", b"3")])
    with pytest.raises(SimulatedCrash):
        inbox.ingest("p", "s", "k", items)
    monkeypatch.undo()

    # 崩溃现场：暂存区留有半成品。
    assert len(listdir(staging_dir(root))) == 1

    inbox.recover()
    assert inbox.query_session("p", "s") == []
    assert listdir(staging_dir(root)) == []
    assert listdir(content_dir(root)) == []

    # 恢复后同键可重新提交（此前从未成功返回）。
    receipt = inbox.ingest("p", "s", "k", items)
    assert len(receipt.files) == 3


def test_crash_after_commit_before_move_completes(inbox, src_dir, root, monkeypatch):
    """内容已保存、回执尚未返回：恢复后完整提交，重试命中同一批次。"""
    def boom(*args, **kwargs):
        raise SimulatedCrash()

    monkeypatch.setattr(lib.os, "replace", boom)
    items = make_items(src_dir, [("a.dwg", b"1"), ("b.dwg", b"2")])
    with pytest.raises(SimulatedCrash):
        inbox.ingest("p", "s", "k", items)
    monkeypatch.undo()

    # 崩溃现场：批次已提交，内容仍在暂存区。
    assert len(listdir(staging_dir(root))) == 1
    assert listdir(content_dir(root)) == []

    # 恢复不依赖原始源文件。
    for it in items:
        os.remove(it.source_path)
    inbox.recover()

    views = inbox.query_session("p", "s")
    assert len(views) == 1
    assert len(views[0].files) == 2
    assert sorted(listdir(content_dir(root))) == sorted(f.file_id for f in views[0].files)
    for fv, data in zip(views[0].files, (b"1", b"2")):
        with inbox.open_file(fv.file_id) as f:
            assert read_all(f) == data

    # 恢复成已提交后，有效期内同键重试命中同一批次。
    items2 = make_items(src_dir, [("a.dwg", b"1"), ("b.dwg", b"2")])
    receipt = inbox.ingest("p", "s", "k", items2)
    assert receipt.batch_id == views[0].batch_id
    assert [f.file_id for f in receipt.files] == [f.file_id for f in views[0].files]


def test_crash_mid_move_completes_remaining(inbox, src_dir, root, monkeypatch):
    """移动到一半被终止：恢复补齐剩余文件。"""
    real_replace = os.replace
    calls = {"n": 0}

    def flaky(src, dst):
        calls["n"] += 1
        if calls["n"] == 2:
            raise SimulatedCrash()
        return real_replace(src, dst)

    monkeypatch.setattr(lib.os, "replace", flaky)
    items = make_items(src_dir, [("a.dwg", b"1"), ("b.dwg", b"2"), ("c.dwg", b"3")])
    with pytest.raises(SimulatedCrash):
        inbox.ingest("p", "s", "k", items)
    monkeypatch.undo()

    assert len(listdir(content_dir(root))) == 1  # 只搬走了一个
    inbox.recover()
    views = inbox.query_session("p", "s")
    assert len(views) == 1 and len(views[0].files) == 3
    assert len(listdir(content_dir(root))) == 3
    assert listdir(staging_dir(root)) == []


def test_recovery_is_repeatable_and_cleans_orphans(inbox, src_dir, root):
    receipt = inbox.ingest("p", "s", "k", make_items(src_dir, [("a.dwg", b"1")]))

    # 手工制造无主内容文件与空暂存目录。
    orphan = os.path.join(content_dir(root), "orphan-file")
    with open(orphan, "wb") as f:
        f.write(b"junk")
    os.makedirs(os.path.join(staging_dir(root), "dead-batch"))
    with open(os.path.join(staging_dir(root), "dead-batch", "0"), "wb") as f:
        f.write(b"partial")

    inbox.recover()
    inbox.recover()
    inbox.recover()

    assert listdir(content_dir(root)) == [receipt.files[0].file_id]
    assert listdir(staging_dir(root)) == []
    with inbox.open_file(receipt.files[0].file_id) as f:
        assert read_all(f) == b"1"


def test_crash_during_recovery_is_safe(inbox, src_dir, root, monkeypatch):
    """恢复本身被中断：再次恢复后结果一致。"""
    def boom(*args, **kwargs):
        raise SimulatedCrash()

    monkeypatch.setattr(lib.os, "replace", boom)
    items = make_items(src_dir, [("a.dwg", b"1"), ("b.dwg", b"2")])
    with pytest.raises(SimulatedCrash):
        inbox.ingest("p", "s", "k", items)
    monkeypatch.undo()

    # 第一次恢复在补齐移动时再次崩溃。
    real_replace = os.replace
    calls = {"n": 0}

    def flaky(src, dst):
        calls["n"] += 1
        if calls["n"] == 2:
            raise SimulatedCrash()
        return real_replace(src, dst)

    monkeypatch.setattr(lib.os, "replace", flaky)
    with pytest.raises(SimulatedCrash):
        inbox.recover()
    monkeypatch.undo()

    inbox.recover()
    views = inbox.query_session("p", "s")
    assert len(views) == 1 and len(views[0].files) == 2
    assert len(listdir(content_dir(root))) == 2
    assert listdir(staging_dir(root)) == []


def test_expired_key_reuse_interrupted_keeps_old_batch(inbox, src_dir, clock, root, monkeypatch):
    """过期键复用过程中崩溃：旧批次始终可读，新批次全有或全无。"""
    old_receipt = inbox.ingest("p", "s", "k", make_items(src_dir, [("old.dwg", b"OLD")]))
    clock.advance(IDEMPOTENCY_TTL)  # 恰好满 24 小时，键可复用

    # 复用键提交新内容，在提交后、移动前崩溃。
    def boom(*args, **kwargs):
        raise SimulatedCrash()

    monkeypatch.setattr(lib.os, "replace", boom)
    new_items = make_items(src_dir, [("new.dwg", b"NEW")])
    with pytest.raises(SimulatedCrash):
        inbox.ingest("p", "s", "k", new_items)
    monkeypatch.undo()

    # 崩溃期间旧批次始终可读。
    with inbox.open_file(old_receipt.files[0].file_id) as f:
        assert read_all(f) == b"OLD"

    inbox.recover()
    views = inbox.query_session("p", "s")
    assert len(views) == 2  # 旧批次 + 恢复补交的新批次
    with inbox.open_file(old_receipt.files[0].file_id) as f:
        assert read_all(f) == b"OLD"
    new_view = [v for v in views if v.batch_id != old_receipt.batch_id][0]
    with inbox.open_file(new_view.files[0].file_id) as f:
        assert read_all(f) == b"NEW"

    # 新批次在自身有效期内，同键同内容重试命中新批次而非旧批次。
    retry = inbox.ingest("p", "s", "k", make_items(src_dir, [("new.dwg", b"NEW")]))
    assert retry.batch_id == new_view.batch_id

    # 过期不等于文件过期：恢复不会清理旧批次内容。
    assert old_receipt.files[0].file_id in listdir(content_dir(root))


def test_change_request_does_not_pollute_live_batch(inbox, src_dir, root):
    """有效期内的变更请求被拒绝，且不改变原批次与内容文件。"""
    receipt = inbox.ingest("p", "s", "k", make_items(src_dir, [("a.dwg", b"1")]))
    before = sorted(listdir(content_dir(root)))

    from cad_inbox import IdempotencyConflictError

    with pytest.raises(IdempotencyConflictError):
        inbox.ingest("p", "s", "k", make_items(src_dir, [("a.dwg", b"DIFFERENT")]))

    inbox.recover()
    assert sorted(listdir(content_dir(root))) == before
    assert listdir(staging_dir(root)) == []
    views = inbox.query_session("p", "s")
    assert len(views) == 1
    assert views[0].files[0].file_id == receipt.files[0].file_id
