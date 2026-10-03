"""基础收件、幂等、重复警告、查询与读回。"""
import os

import pytest

from cad_inbox import (
    BatchItemRequest,
    CadInbox,
    IdempotencyConflictError,
    SourceFileError,
    ValidationError,
)
from cad_inbox.inbox import IDEMPOTENCY_TTL_SECONDS

from conftest import make_source, read_all, sha256_of


def test_submit_and_readback(inbox, tmp_path):
    data = [os.urandom(1024), os.urandom(2048), os.urandom(1)]
    reqs = [
        BatchItemRequest(f"part{i}.dwg", make_source(tmp_path, f"part{i}.dwg", d))
        for i, d in enumerate(data)
    ]
    receipt = inbox.submit_batch("p1", "s1", "k1", reqs)

    assert len(receipt.items) == 3
    assert len({it.file_id for it in receipt.items}) == 3  # 每项独立文件标识
    entries = inbox.list_session_files("p1", "s1")
    assert [e.file_id for e in entries] == [it.file_id for it in receipt.items]
    for item, original in zip(receipt.items, data):
        assert item.sha256 == sha256_of(original)
        assert item.size == len(original)
        with inbox.open_file(item.file_id) as fh:
            assert fh.read() == original


def test_source_deletable_after_success(inbox, tmp_path):
    data = os.urandom(4096)
    src = make_source(tmp_path, "a.dwg", data)
    receipt = inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", src)])
    os.remove(src)  # 成功返回后调用方立即删除源文件
    with inbox.open_file(receipt.items[0].file_id) as fh:
        assert fh.read() == data


def test_same_content_different_submissions_kept_separately(inbox, tmp_path):
    """内容相同是有意保留的不同交付件：各自记录、分别读回。"""
    data = b"identical payload"
    src = make_source(tmp_path, "model.dwg", data)
    r1 = inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("v1.dwg", src)])
    r2 = inbox.submit_batch("p1", "s1", "k2", [BatchItemRequest("v2.dwg", src)])

    assert r1.batch_id != r2.batch_id
    assert r1.items[0].file_id != r2.items[0].file_id
    assert r1.items[0].duplicate is False  # 首次提交时无重复
    assert r2.items[0].duplicate is True  # 与此前已提交文件校验和相同 -> 警告但不阻止
    for item in (r1.items[0], r2.items[0]):
        with inbox.open_file(item.file_id) as fh:
            assert fh.read() == data


def test_duplicate_warning_within_batch(inbox, tmp_path):
    data = b"dup"
    s1 = make_source(tmp_path, "a.dwg", data)
    s2 = make_source(tmp_path, "b.dwg", data)
    s3 = make_source(tmp_path, "c.dwg", b"unique")
    receipt = inbox.submit_batch(
        "p1", "s1", "k1",
        [BatchItemRequest("a.dwg", s1), BatchItemRequest("b.dwg", s2),
         BatchItemRequest("c.dwg", s3)],
    )
    assert [it.duplicate for it in receipt.items] == [False, True, False]


def test_duplicate_scoped_per_project(inbox, tmp_path):
    data = b"shared"
    src = make_source(tmp_path, "a.dwg", data)
    inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", src)])
    other = inbox.submit_batch("p2", "s1", "k1", [BatchItemRequest("a.dwg", src)])
    assert other.items[0].duplicate is False  # 跨项目不算重复


def test_idempotent_retry_returns_original_receipt(inbox, tmp_path):
    src = make_source(tmp_path, "a.dwg", b"data-a")
    reqs = [BatchItemRequest("a.dwg", src)]
    r1 = inbox.submit_batch("p1", "s1", "k1", reqs)
    r2 = inbox.submit_batch("p1", "s1", "k1", reqs)
    assert r1 == r2  # 回执稳定：批次、文件标识、提交时间完全一致
    assert len(inbox.list_session_files("p1", "s1")) == 1  # 不新增文件
    os.remove(src)  # 成功返回后源文件可删除，不影响读回
    with inbox.open_file(r1.items[0].file_id) as fh:
        assert fh.read() == b"data-a"


def test_retry_with_different_content_rejected(inbox, tmp_path):
    src = make_source(tmp_path, "a.dwg", b"data-a")
    inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", src)])
    other = make_source(tmp_path, "b.dwg", b"data-b")
    with pytest.raises(IdempotencyConflictError):
        inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("b.dwg", other)])
    # 原批次不被污染
    entries = inbox.list_session_files("p1", "s1")
    assert len(entries) == 1 and entries[0].filename == "a.dwg"


def test_retry_with_different_order_rejected(inbox, tmp_path):
    s1 = make_source(tmp_path, "a.dwg", b"aaa")
    s2 = make_source(tmp_path, "b.dwg", b"bbb")
    inbox.submit_batch("p1", "s1", "k1",
                       [BatchItemRequest("a.dwg", s1), BatchItemRequest("b.dwg", s2)])
    with pytest.raises(IdempotencyConflictError):
        inbox.submit_batch("p1", "s1", "k1",
                           [BatchItemRequest("b.dwg", s2), BatchItemRequest("a.dwg", s1)])


def test_source_path_not_part_of_identity(inbox, tmp_path):
    """同内容同文件名、不同源路径：命中同一回执。"""
    data = b"same bytes"
    s1 = make_source(tmp_path, "x/a.dwg", data)
    s2 = make_source(tmp_path, "y/a.dwg", data)
    r1 = inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", s1)])
    r2 = inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", s2)])
    assert r1 == r2


def test_idempotency_key_scoped_by_project_and_session(inbox, tmp_path):
    src = make_source(tmp_path, "a.dwg", b"data")
    r1 = inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", src)])
    r2 = inbox.submit_batch("p1", "s2", "k1", [BatchItemRequest("a.dwg", src)])
    r3 = inbox.submit_batch("p2", "s1", "k1", [BatchItemRequest("a.dwg", src)])
    assert len({r1.batch_id, r2.batch_id, r3.batch_id}) == 3


def test_idempotency_boundary_24h(inbox, tmp_path, clock):
    src = make_source(tmp_path, "a.dwg", b"data")
    reqs = [BatchItemRequest("a.dwg", src)]
    r1 = inbox.submit_batch("p1", "s1", "k1", reqs)

    clock.advance(IDEMPOTENCY_TTL_SECONDS - 1)  # 差 1 秒满 24h：仍命中原回执
    assert inbox.submit_batch("p1", "s1", "k1", reqs) == r1

    clock.advance(1)  # 恰好满 24h：同一键代表新批次
    r2 = inbox.submit_batch("p1", "s1", "k1", reqs)
    assert r2.batch_id != r1.batch_id
    assert r2.items[0].file_id != r1.items[0].file_id
    assert r2.items[0].duplicate is True  # 与旧批次内容相同 -> 警告
    # 旧批次仍然保留、可读
    assert len(inbox.list_session_files("p1", "s1")) == 2
    with inbox.open_file(r1.items[0].file_id) as fh:
        assert fh.read() == b"data"


def test_warnings_frozen_at_first_submit(inbox, tmp_path, clock):
    """重试不得因后来新增文件而改变警告、文件标识或提交时间。"""
    s1 = make_source(tmp_path, "a.dwg", b"A")
    r1 = inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", s1)])
    assert r1.items[0].duplicate is False

    # 另一项目键提交同内容文件，使"现在看"应判重复
    s2 = make_source(tmp_path, "b.dwg", b"A")
    inbox.submit_batch("p1", "s1", "k2", [BatchItemRequest("b.dwg", s2)])

    r1_retry = inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("a.dwg", s1)])
    assert r1_retry == r1
    assert r1_retry.items[0].duplicate is False  # 以首次提交时的状态为准


def test_batch_visible_atomically(inbox, tmp_path):
    """查询只能看到整个批次。提交前后对比验证。"""
    reqs = [
        BatchItemRequest(f"f{i}.dwg", make_source(tmp_path, f"f{i}.dwg", bytes([i]) * 8))
        for i in range(5)
    ]
    assert inbox.list_session_files("p1", "s1") == []
    receipt = inbox.submit_batch("p1", "s1", "k1", reqs)
    entries = inbox.list_session_files("p1", "s1")
    assert len(entries) == 5
    assert {e.batch_id for e in entries} == {receipt.batch_id}


def test_validation_limits(inbox, tmp_path):
    src = make_source(tmp_path, "a.dwg", b"x")
    with pytest.raises(ValidationError):
        inbox.submit_batch("p1", "s1", "k1", [])
    too_many = [BatchItemRequest(f"f{i}", src) for i in range(51)]
    with pytest.raises(ValidationError):
        inbox.submit_batch("p1", "s1", "k1", too_many)
    with pytest.raises(ValidationError):
        inbox.submit_batch("", "s1", "k1", [BatchItemRequest("a", src)])
    with pytest.raises(SourceFileError):
        inbox.submit_batch("p1", "s1", "k1",
                           [BatchItemRequest("a", str(tmp_path / "missing"))])


def test_oversize_file_rejected(inbox, tmp_path):
    big = tmp_path / "src" / "big.dwg"
    big.parent.mkdir(parents=True, exist_ok=True)
    with open(big, "wb") as fh:
        fh.seek(200 * 1024 * 1024)  # 200 MiB + 1
        fh.write(b"\0")
    with pytest.raises(ValidationError):
        inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("big.dwg", str(big))])


def test_readback_via_open_file_streaming(inbox, tmp_path):
    data = os.urandom(3 * 1024 * 1024 + 7)
    src = make_source(tmp_path, "big.dwg", data)
    receipt = inbox.submit_batch("p1", "s1", "k1", [BatchItemRequest("big.dwg", src)])
    with inbox.open_file(receipt.items[0].file_id) as fh:
        assert read_all(fh, chunk=65536) == data
