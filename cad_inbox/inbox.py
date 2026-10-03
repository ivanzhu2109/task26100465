"""CAD 评审文件收件库主实现。

存储布局（root_dir 下）：
    inbox.db           SQLite，保存批次/文件元数据与回执
    files/<batch_id>/  已提交批次的内容文件（按序号命名）
    staging/<batch_id>/ 收件过程中的临时内容文件

提交点：SQLite 事务把批次状态置为 'committed'。此前崩溃由 recover()
回滚（删除半成品文件与行）；此后崩溃不影响已提交的批次。
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import time
import uuid
from typing import BinaryIO, Callable, Iterable, List, Optional

from .errors import IdempotencyConflictError, SourceFileError, ValidationError
from .models import BatchItemRequest, FileEntry, Receipt, ReceiptItem

IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60
MAX_BATCH_ITEMS = 50
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 MiB
_CHUNK_SIZE = 1024 * 1024  # 1 MiB，流式读写，控制内存峰值

_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    project_id   TEXT NOT NULL,
    session_id   TEXT NOT NULL,
    idem_key     TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('staging', 'committed')),
    submitted_at REAL,
    receipt_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_key
    ON batches (project_id, session_id, idem_key);
CREATE TABLE IF NOT EXISTS items (
    batch_id  TEXT NOT NULL REFERENCES batches (batch_id),
    idx       INTEGER NOT NULL,
    file_id   TEXT NOT NULL,
    filename  TEXT NOT NULL,
    sha256    TEXT NOT NULL,
    size      INTEGER NOT NULL,
    duplicate INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (batch_id, idx)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_items_file_id ON items (file_id);
CREATE INDEX IF NOT EXISTS idx_items_sha256 ON items (sha256);
"""


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _hash_file(path: str) -> tuple[str, int]:
    """流式计算 SHA-256，返回 (hex digest, 字节数)。"""
    digest = hashlib.sha256()
    total = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


class CadInbox:
    """文件收件库：批量收件、查询读回、崩溃恢复。

    now: 可注入的时钟（返回 Unix 秒），便于测试 24 小时幂等边界。
    crash_hook: 测试用故障注入钩子，在关键步骤被调用，可抛出异常模拟进程中断。
    """

    def __init__(
        self,
        root_dir: str,
        now: Optional[Callable[[], float]] = None,
        crash_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._root = os.path.abspath(root_dir)
        self._now = now or time.time
        self._crash_hook = crash_hook or (lambda point: None)
        self._files_dir = os.path.join(self._root, "files")
        self._staging_dir = os.path.join(self._root, "staging")
        os.makedirs(self._files_dir, exist_ok=True)
        os.makedirs(self._staging_dir, exist_ok=True)
        self._db = sqlite3.connect(os.path.join(self._root, "inbox.db"))
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.executescript(_SCHEMA)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "CadInbox":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # 以下方法在后续步骤中填充实现
    # ------------------------------------------------------------------

    def recover(self) -> None:
        """恢复中断的收件/恢复流程；可反复执行，幂等。

        策略：未提交的批次一律回滚（允许完全撤回）。先删库内未完成
        批次的行，再清理不属于已提交批次的内容目录；任何一步中断，
        下次 recover 都能自愈，且绝不触碰已提交批次的内容。
        """
        with self._db:
            self._db.execute(
                "DELETE FROM items WHERE batch_id IN"
                " (SELECT batch_id FROM batches WHERE state != 'committed')"
            )
            self._db.execute("DELETE FROM batches WHERE state != 'committed'")
        self._crash_hook("recover_after_delete_rows")

        committed = {
            row[0]
            for row in self._db.execute(
                "SELECT batch_id FROM batches WHERE state = 'committed'"
            )
        }
        for base in (self._staging_dir, self._files_dir):
            for name in os.listdir(base):
                path = os.path.join(base, name)
                if name not in committed and os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                    self._crash_hook("recover_after_remove_dir")
            _fsync_dir(base)
        self._crash_hook("recover_done")

    # ------------------------------------------------------------------
    # 批量收件
    # ------------------------------------------------------------------

    def submit_batch(
        self,
        project_id: str,
        session_id: str,
        idempotency_key: str,
        files: Iterable[BatchItemRequest],
    ) -> Receipt:
        requests = self._validate_request(project_id, session_id, idempotency_key, files)

        # 阶段 1：流式计算每项 SHA-256（源路径不属于请求身份，只取内容）。
        hashed = []
        for req in requests:
            sha256, size = _hash_file(req.source_path)
            hashed.append((req.filename, sha256, size))
        request_hash = self._request_hash(hashed)

        # 阶段 2：幂等检查。有效期内同键同请求 -> 返回原回执；不同 -> 拒绝。
        existing = self._find_live_batch(project_id, session_id, idempotency_key)
        if existing is not None:
            if existing[0] == request_hash:
                return self._receipt_from_json(existing[1])
            raise IdempotencyConflictError(
                "幂等键仍在有效期内，但请求内容与首次提交不一致"
            )

        # 阶段 3：登记 staging 批次（崩溃后由 recover 回滚）。
        batch_id = uuid.uuid4().hex
        items = [
            (batch_id, idx, uuid.uuid4().hex, filename, sha256, size)
            for idx, (filename, sha256, size) in enumerate(hashed)
        ]
        with self._db:
            self._db.execute(
                "INSERT INTO batches (batch_id, project_id, session_id, idem_key,"
                " request_hash, state) VALUES (?, ?, ?, ?, ?, 'staging')",
                (batch_id, project_id, session_id, idempotency_key, request_hash),
            )
            self._db.executemany(
                "INSERT INTO items (batch_id, idx, file_id, filename, sha256, size)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                items,
            )
        self._crash_hook("after_staging_rows")

        # 阶段 4：逐项复制内容到 staging 目录，边复制边复核哈希。
        staging_path = os.path.join(self._staging_dir, batch_id)
        os.makedirs(staging_path)
        try:
            for idx, req in enumerate(requests):
                expected_sha, expected_size = hashed[idx][1], hashed[idx][2]
                actual_sha, actual_size = self._copy_into(
                    req.source_path, os.path.join(staging_path, f"{idx:06d}")
                )
                if (actual_sha, actual_size) != (expected_sha, expected_size):
                    raise SourceFileError(f"源文件在收件过程中发生变化: {req.source_path}")
                self._crash_hook("after_stage_file")
            _fsync_dir(staging_path)
        except BaseException:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise
        self._crash_hook("after_stage_files")

        # 阶段 5：计算重复警告（以首次提交时的状态为准，之后冻结在库里）。
        seen_in_batch: set[str] = set()
        duplicates = []
        for _, sha256, _ in hashed:
            dup = sha256 in seen_in_batch or self._is_committed_in_project(
                project_id, sha256
            )
            duplicates.append(dup)
            seen_in_batch.add(sha256)
        submitted_at = self._now()
        receipt = Receipt(
            batch_id=batch_id,
            project_id=project_id,
            session_id=session_id,
            idempotency_key=idempotency_key,
            submitted_at=submitted_at,
            items=tuple(
                ReceiptItem(
                    file_id=row[2],
                    filename=row[3],
                    sha256=row[4],
                    size=row[5],
                    duplicate=duplicates[idx],
                )
                for idx, row in enumerate(items)
            ),
        )

        # 阶段 6：内容文件就位（files/<batch_id>/），再落库提交。
        os.rename(staging_path, os.path.join(self._files_dir, batch_id))
        _fsync_dir(self._files_dir)
        _fsync_dir(self._staging_dir)
        self._crash_hook("after_move_files")
        with self._db:
            self._db.executemany(
                "UPDATE items SET duplicate = ? WHERE batch_id = ? AND idx = ?",
                [
                    (1 if dup else 0, batch_id, idx)
                    for idx, dup in enumerate(duplicates)
                ],
            )
            self._db.execute(
                "UPDATE batches SET state = 'committed', submitted_at = ?,"
                " receipt_json = ? WHERE batch_id = ?",
                (submitted_at, self._receipt_to_json(receipt), batch_id),
            )
        self._crash_hook("after_commit")
        return receipt

    # ------------------------------------------------------------------
    # submit_batch 的内部辅助
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_request(
        project_id: str,
        session_id: str,
        idempotency_key: str,
        files: Iterable[BatchItemRequest],
    ) -> List[BatchItemRequest]:
        if not project_id or not session_id or not idempotency_key:
            raise ValidationError("项目标识、会话标识、幂等键均不能为空")
        requests = list(files)
        if not 1 <= len(requests) <= MAX_BATCH_ITEMS:
            raise ValidationError(f"每批文件数量须在 1 至 {MAX_BATCH_ITEMS} 之间")
        for req in requests:
            if not req.filename or "\0" in req.filename:
                raise ValidationError("文件名不能为空")
            try:
                st = os.stat(req.source_path)
            except OSError as exc:
                raise SourceFileError(f"源文件不可读: {req.source_path}") from exc
            if not os.path.isfile(req.source_path):
                raise SourceFileError(f"源文件不是普通文件: {req.source_path}")
            if st.st_size > MAX_FILE_SIZE:
                raise ValidationError(
                    f"单文件超过 200 MiB 上限: {req.filename} ({st.st_size} 字节)"
                )
        return requests

    @staticmethod
    def _request_hash(hashed: List[tuple[str, str, int]]) -> str:
        canonical = json.dumps(
            [[filename, sha256, size] for filename, sha256, size in hashed],
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _find_live_batch(
        self, project_id: str, session_id: str, idempotency_key: str
    ) -> Optional[tuple[str, str]]:
        """返回有效期内最近一次提交的 (request_hash, receipt_json)，无则 None。"""
        cutoff = self._now() - IDEMPOTENCY_TTL_SECONDS
        row = self._db.execute(
            "SELECT request_hash, receipt_json FROM batches"
            " WHERE project_id = ? AND session_id = ? AND idem_key = ?"
            " AND state = 'committed' AND submitted_at > ?"
            " ORDER BY submitted_at DESC LIMIT 1",
            (project_id, session_id, idempotency_key, cutoff),
        ).fetchone()
        return (row[0], row[1]) if row else None

    def _is_committed_in_project(self, project_id: str, sha256: str) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM items i JOIN batches b ON i.batch_id = b.batch_id"
            " WHERE b.project_id = ? AND b.state = 'committed' AND i.sha256 = ?"
            " LIMIT 1",
            (project_id, sha256),
        ).fetchone()
        return row is not None

    @staticmethod
    def _copy_into(source_path: str, dest_path: str) -> tuple[str, int]:
        """流式复制并计算 SHA-256，落盘后 fsync。"""
        digest = hashlib.sha256()
        total = 0
        with open(source_path, "rb") as src, open(dest_path, "wb") as dst:
            while True:
                chunk = src.read(_CHUNK_SIZE)
                if not chunk:
                    break
                digest.update(chunk)
                dst.write(chunk)
                total += len(chunk)
            dst.flush()
            os.fsync(dst.fileno())
        return digest.hexdigest(), total

    @staticmethod
    def _receipt_to_json(receipt: Receipt) -> str:
        return json.dumps(
            {
                "batch_id": receipt.batch_id,
                "project_id": receipt.project_id,
                "session_id": receipt.session_id,
                "idempotency_key": receipt.idempotency_key,
                "submitted_at": receipt.submitted_at,
                "items": [
                    {
                        "file_id": it.file_id,
                        "filename": it.filename,
                        "sha256": it.sha256,
                        "size": it.size,
                        "duplicate": it.duplicate,
                    }
                    for it in receipt.items
                ],
            },
            separators=(",", ":"),
        )

    @staticmethod
    def _receipt_from_json(payload: str) -> Receipt:
        data = json.loads(payload)
        return Receipt(
            batch_id=data["batch_id"],
            project_id=data["project_id"],
            session_id=data["session_id"],
            idempotency_key=data["idempotency_key"],
            submitted_at=data["submitted_at"],
            items=tuple(
                ReceiptItem(
                    file_id=it["file_id"],
                    filename=it["filename"],
                    sha256=it["sha256"],
                    size=it["size"],
                    duplicate=it["duplicate"],
                )
                for it in data["items"]
            ),
        )

    def list_session_files(self, project_id: str, session_id: str) -> List[FileEntry]:
        """查询会话文件：只返回已提交批次，批次整体可见。"""
        rows = self._db.execute(
            "SELECT i.file_id, i.batch_id, i.filename, i.sha256, i.size,"
            " i.duplicate, b.submitted_at"
            " FROM items i JOIN batches b ON i.batch_id = b.batch_id"
            " WHERE b.project_id = ? AND b.session_id = ? AND b.state = 'committed'"
            " ORDER BY b.submitted_at, i.batch_id, i.idx",
            (project_id, session_id),
        ).fetchall()
        return [
            FileEntry(
                file_id=row[0],
                batch_id=row[1],
                filename=row[2],
                sha256=row[3],
                size=row[4],
                duplicate=bool(row[5]),
                submitted_at=row[6],
            )
            for row in rows
        ]

    def open_file(self, file_id: str) -> BinaryIO:
        """按文件标识读回内容（流式二进制句柄，调用方负责关闭）。"""
        row = self._db.execute(
            "SELECT i.batch_id, i.idx FROM items i"
            " JOIN batches b ON i.batch_id = b.batch_id"
            " WHERE i.file_id = ? AND b.state = 'committed'",
            (file_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"未知的文件标识: {file_id}")
        path = os.path.join(self._files_dir, row[0], f"{row[1]:06d}")
        return open(path, "rb")
