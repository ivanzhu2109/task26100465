"""收件、幂等、重复警告、24 小时边界的测试。"""

import os

import pytest

from cad_inbox import (
    CadInbox,
    IdempotencyConflictError,
    IngestItem,
    NotFoundError,
    ValidationError,
)
from cad_inbox.library import IDEMPOTENCY_TTL

from conftest import make_items, make_source, read_all, sha256_of


def test_ingest_returns_receipt_and_files_readable(inbox, src_dir):
    items = make_items(src_dir, [("a.dwg", b"AAAA"), ("b.dwg", b"BBBB")])
    receipt = inbox.ingest("proj", "sess", "k1", items)

    assert receipt.project_id == "proj"
    assert receipt.session_id == "sess"
    assert receipt.idempotency_key == "k1"
    assert len(receipt.files) == 2
    assert receipt.files[0].filename == "a.dwg"
    assert receipt.files[0].sha256 == sha256_of(b"AAAA")
    assert receipt.files[0].size == 4
    assert all(not f.duplicate_warning for f in receipt.files)

    # 成功返回后调用方可立即删除全部源文件。
    for it in items:
        os.remove(it.source_path)
    with inbox.open_file(receipt.files[0].file_id) as f:
        assert read_all(f) == b"AAAA"
    with inbox.open_file(receipt.files[1].file_id) as f:
        assert read_all(f) == b"BBBB"


def test_extract_file_roundtrip(inbox, src_dir, tmp_path):
    items = make_items(src_dir, [("x.dwg", b"X" * 5000)])
    receipt = inbox.ingest("p", "s", "k", items)
    dest = str(tmp_path / "out.dwg")
    inbox.extract_file(receipt.files[0].file_id, dest)
    with open(dest, "rb") as f:
        assert f.read() == b"X" * 5000


def test_same_content_kept_as_separate_records(inbox, src_dir):
    """相同内容的有意提交保留各自记录，可分别读回。"""
    items = make_items(src_dir, [("v1.dwg", b"SAME"), ("v2.dwg", b"SAME")])
    receipt = inbox.ingest("p", "s", "k", items)
    assert receipt.files[0].file_id != receipt.files[1].file_id
    assert receipt.files[0].duplicate_warning is False
    assert receipt.files[1].duplicate_warning is True  # 与批内更早项相同

    for f in receipt.files:
        with inbox.open_file(f.file_id) as stream:
            assert read_all(stream) == b"SAME"


def test_duplicate_warning_against_earlier_batch(inbox, src_dir):
    """与此前批次同内容 → 警告但不阻止收件。"""
    r1 = inbox.ingest("p", "s", "k1", make_items(src_dir, [("a.dwg", b"DUP")]))
    assert r1.files[0].duplicate_warning is False

    r2 = inbox.ingest("p", "s", "k2", make_items(src_dir, [("b.dwg", b"DUP")]))
    assert r2.files[0].duplicate_warning is True
    assert r2.files[0].file_id != r1.files[0].file_id

    # 不同项目互不影响。
    r3 = inbox.ingest("other", "s", "k3", make_items(src_dir, [("c.dwg", b"DUP")]))
    assert r3.files[0].duplicate_warning is False


def test_idempotent_retry_returns_same_receipt(inbox, src_dir):
    items = make_items(src_dir, [("a.dwg", b"1"), ("b.dwg", b"2")])
    r1 = inbox.ingest("p", "s", "k", items)
    r2 = inbox.ingest("p", "s", "k", items)
    assert r1 == r2
    # 不新增文件：会话中仍只有一个批次、两个文件。
    views = inbox.query_session("p", "s")
    assert len(views) == 1
    assert len(views[0].files) == 2


def test_idempotency_scoped_by_project_and_session(inbox, src_dir):
    r1 = inbox.ingest("p1", "s", "k", make_items(src_dir, [("a.dwg", b"1")]))
    r2 = inbox.ingest("p2", "s", "k", make_items(src_dir, [("a.dwg", b"1")]))
    r3 = inbox.ingest("p1", "s2", "k", make_items(src_dir, [("a.dwg", b"1")]))
    assert len({r1.batch_id, r2.batch_id, r3.batch_id}) == 3


def test_conflicting_request_rejected(inbox, src_dir):
    inbox.ingest("p", "s", "k", make_items(src_dir, [("a.dwg", b"1")]))

    # 内容不同
    with pytest.raises(IdempotencyConflictError):
        inbox.ingest("p", "s", "k", make_items(src_dir, [("a.dwg", b"CHANGED")]))
    # 文件名不同
    with pytest.raises(IdempotencyConflictError):
        inbox.ingest("p", "s", "k", make_items(src_dir, [("renamed.dwg", b"1")]))
    # 顺序不同
    inbox.ingest("p", "s", "k2", make_items(src_dir, [("a.dwg", b"1"), ("b.dwg", b"2")]))
    with pytest.raises(IdempotencyConflictError):
        inbox.ingest("p", "s", "k2", make_items(src_dir, [("b.dwg", b"2"), ("a.dwg", b"1")]))

    # 拒绝不改变已有结果。
    views = inbox.query_session("p", "s")
    assert len(views) == 2
    assert [f.filename for f in views[0].files] == ["a.dwg"]


def test_source_path_not_part_of_identity(inbox, src_dir, tmp_path):
    """同一内容从不同源路径提交，仍命中幂等。"""
    other = tmp_path / "elsewhere"
    other.mkdir()
    p1 = make_source(src_dir, "a.dwg", b"DATA")
    p2 = make_source(str(other), "a.dwg", b"DATA")
    r1 = inbox.ingest("p", "s", "k", [IngestItem("a.dwg", p1)])
    r2 = inbox.ingest("p", "s", "k", [IngestItem("a.dwg", p2)])
    assert r1 == r2


def test_expiry_boundary(inbox, src_dir, clock):
    r1 = inbox.ingest("p", "s", "k", make_items(src_dir, [("a.dwg", b"1")]))

    # 差 1 秒满 24 小时：仍命中原回执。
    clock.advance(IDEMPOTENCY_TTL - 1)
    r2 = inbox.ingest("p", "s", "k", make_items(src_dir, [("a.dwg", b"1")]))
    assert r2 == r1

    # 恰好满 24 小时：同一键代表新批次，旧批次保留。
    clock.advance(1)
    r3 = inbox.ingest("p", "s", "k", make_items(src_dir, [("a.dwg", b"1")]))
    assert r3.batch_id != r1.batch_id
    assert r3.files[0].file_id != r1.files[0].file_id
    # 内容相同 → 新批次带重复警告（以首次提交时状态为准，旧回执不变）。
    assert r3.files[0].duplicate_warning is True
    assert r1.files[0].duplicate_warning is False

    views = inbox.query_session("p", "s")
    assert len(views) == 2
    with inbox.open_file(r1.files[0].file_id) as f:
        assert read_all(f) == b"1"


def test_query_session_isolation(inbox, src_dir):
    inbox.ingest("p", "s1", "k1", make_items(src_dir, [("a.dwg", b"1")]))
    inbox.ingest("p", "s2", "k2", make_items(src_dir, [("b.dwg", b"2")]))
    inbox.ingest("q", "s1", "k3", make_items(src_dir, [("c.dwg", b"3")]))
    assert len(inbox.query_session("p", "s1")) == 1
    assert len(inbox.query_session("p", "s2")) == 1
    assert inbox.query_session("p", "nope") == []


def test_open_unknown_file_raises(inbox):
    with pytest.raises(NotFoundError):
        inbox.open_file("no-such-id")


def test_validation(inbox, src_dir):
    with pytest.raises(ValidationError):
        inbox.ingest("p", "s", "k", [])
    with pytest.raises(ValidationError):
        inbox.ingest("p", "s", "k", make_items(src_dir, [(f"f{i}", b"x") for i in range(51)]))
    with pytest.raises(ValidationError):
        inbox.ingest("", "s", "k", make_items(src_dir, [("a", b"x")]))
    with pytest.raises(ValidationError):
        inbox.ingest("p", "s", "k", [IngestItem("a.dwg", "/no/such/file")])
    with pytest.raises(ValidationError):
        inbox.ingest("p", "s", "k", [IngestItem("", make_source(src_dir, "a", b"x"))])

    big = make_source(src_dir, "big.dwg", b"")
    with open(big, "wb") as f:
        f.truncate(200 * 1024 * 1024 + 1)
    with pytest.raises(ValidationError):
        inbox.ingest("p", "s", "k", [IngestItem("big.dwg", big)])

    # 验证失败不留下任何批次。
    assert inbox.query_session("p", "s") == []
