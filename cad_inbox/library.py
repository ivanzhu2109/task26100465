"""CAD 评审文件收件库：对外主入口。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import typing
import uuid

from .errors import IdempotencyConflictError, NotFoundError, ValidationError
from .models import BatchView, FileReceipt, FileView, IngestItem, Receipt
from .storage import Storage

MAX_FILES_PER_BATCH = 50
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 MiB
IDEMPOTENCY_TTL = 24 * 60 * 60  # 24 小时，恰好满 24 小时即视为新批次
COPY_CHUNK_SIZE = 1024 * 1024  # 1 MiB，保证峰值内存受控

_DB_NAME = "inbox.db"
_CONTENT_DIR = "content"
_STAGING_DIR = "staging"


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class CadInbox:
    """收件库句柄。调用串行，不要求并发写入。"""

    def __init__(
        self,
        root_dir: str,
        now: typing.Callable[[], float] | None = None,
    ) -> None:
        self._root = os.path.abspath(root_dir)
        os.makedirs(self._root, exist_ok=True)
        self._content = os.path.join(self._root, _CONTENT_DIR)
        self._staging = os.path.join(self._root, _STAGING_DIR)
        os.makedirs(self._content, exist_ok=True)
        os.makedirs(self._staging, exist_ok=True)
        self._now = now if now is not None else time.time
        self._storage = Storage(os.path.join(self._root, _DB_NAME))

    def close(self) -> None:
        self._storage.close()

    def __enter__(self) -> "CadInbox":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ 收件

    def ingest(
        self,
        project_id: str,
        session_id: str,
        idempotency_key: str,
        items: list[IngestItem],
    ) -> Receipt:
        """批量收件：整批保存内容、提交元数据与回执后返回。"""
        self._validate_request(project_id, session_id, idempotency_key, items)

        batch_id = uuid.uuid4().hex
        staging_dir = os.path.join(self._staging, batch_id)
        os.makedirs(staging_dir)

        # 1. 流式复制到暂存区并计算 SHA-256；源路径不属于请求身份。
        staged: list[dict[str, typing.Any]] = []
        try:
            for seq, item in enumerate(items):
                sha256, size = self._copy_to_staging(item.source_path, staging_dir, seq)
                staged.append(
                    {
                        "seq": seq,
                        "file_id": uuid.uuid4().hex,
                        "filename": item.filename,
                        "sha256": sha256,
                        "size": size,
                    }
                )
            _fsync_dir(staging_dir)
        except Exception:
            # 正常失败（如校验错误）立即清理；进程崩溃留下的半成品由 recover 处理。
            shutil.rmtree(staging_dir, ignore_errors=True)
            raise

        fingerprint = json.dumps(
            [
                {"filename": s["filename"], "sha256": s["sha256"], "size": s["size"]}
                for s in staged
            ],
            separators=(",", ":"),
        )

        # 2. 幂等判定：有效期内同键同内容返回原回执，不同内容拒绝。
        existing = self._storage.find_batch_by_key(
            project_id, session_id, idempotency_key
        )
        now = self._now()
        if existing is not None and now - existing["submitted_at"] < IDEMPOTENCY_TTL:
            shutil.rmtree(staging_dir, ignore_errors=True)
            if existing["fingerprint"] == fingerprint:
                return self._receipt_from_json(existing["receipt_json"])
            raise IdempotencyConflictError(
                f"幂等键 {idempotency_key!r} 在 24 小时有效期内已被不同内容使用"
            )

        # 3. 重复警告：以首次提交时的状态为准，此后不再改变。
        seen_in_batch: set[str] = set()
        for s in staged:
            s["duplicate_warning"] = (
                s["sha256"] in seen_in_batch
                or self._storage.project_has_sha256(project_id, s["sha256"])
            )
            seen_in_batch.add(s["sha256"])

        # 4. 单事务提交批次、文件与回执。
        submitted_at = self._now()
        receipt = Receipt(
            batch_id=batch_id,
            project_id=project_id,
            session_id=session_id,
            idempotency_key=idempotency_key,
            submitted_at=submitted_at,
            files=tuple(
                FileReceipt(
                    file_id=s["file_id"],
                    filename=s["filename"],
                    sha256=s["sha256"],
                    size=s["size"],
                    duplicate_warning=s["duplicate_warning"],
                )
                for s in staged
            ),
        )
        self._storage.commit_batch(
            {
                "batch_id": batch_id,
                "project_id": project_id,
                "session_id": session_id,
                "idempotency_key": idempotency_key,
                "fingerprint": fingerprint,
                "receipt_json": self._receipt_to_json(receipt),
                "submitted_at": submitted_at,
            },
            staged,
        )

        # 5. 提交后把内容文件迁入正式区；崩溃由 recover 补齐。
        for s in staged:
            os.replace(
                os.path.join(staging_dir, str(s["seq"])),
                os.path.join(self._content, s["file_id"]),
            )
        _fsync_dir(self._content)
        shutil.rmtree(staging_dir, ignore_errors=True)
        _fsync_dir(self._staging)
        return receipt

    # ------------------------------------------------------------------ 查询

    def query_session(self, project_id: str, session_id: str) -> list[BatchView]:
        """查询会话，只返回完整批次。"""
        return [
            BatchView(
                batch_id=b["batch_id"],
                idempotency_key=b["idempotency_key"],
                submitted_at=b["submitted_at"],
                files=tuple(
                    FileView(
                        file_id=f["file_id"],
                        filename=f["filename"],
                        sha256=f["sha256"],
                        size=f["size"],
                        duplicate_warning=bool(f["duplicate_warning"]),
                    )
                    for f in b["files"]
                ),
            )
            for b in self._storage.list_session_batches(project_id, session_id)
        ]

    def open_file(self, file_id: str) -> typing.BinaryIO:
        """以只读二进制流读回已提交文件。"""
        row = self._storage.get_file(file_id)
        if row is None:
            raise NotFoundError(f"文件不存在: {file_id}")
        return open(os.path.join(self._content, file_id), "rb")

    def extract_file(self, file_id: str, dest_path: str) -> None:
        """把已提交文件内容流式写出到目标路径。"""
        with self.open_file(file_id) as src, open(dest_path, "wb") as dst:
            shutil.copyfileobj(src, dst, COPY_CHUNK_SIZE)

    # ------------------------------------------------------------------ 恢复

    def recover(self) -> None:
        """崩溃恢复：半成品批次全有或全无，清理无主内容文件。可反复执行。"""
        files = self._storage.list_all_files()

        # 1. 已提交批次：把仍滞留在暂存区的内容文件迁入正式区。
        for f in files:
            dest = os.path.join(self._content, f["file_id"])
            if os.path.exists(dest):
                continue
            src = os.path.join(self._staging, f["batch_id"], str(f["seq"]))
            if not os.path.exists(src):
                raise NotFoundError(
                    f"已提交批次 {f['batch_id']} 缺少内容文件（seq={f['seq']}）"
                )
            os.replace(src, dest)
        _fsync_dir(self._content)

        # 2. 删除所有遗留暂存目录（未提交批次的半成品整体撤回）。
        if os.path.isdir(self._staging):
            for name in os.listdir(self._staging):
                shutil.rmtree(os.path.join(self._staging, name), ignore_errors=True)
            _fsync_dir(self._staging)

        # 3. 删除不属于任何已提交批次的内容文件；键过期不等于文件过期。
        valid_ids = {f["file_id"] for f in files}
        for name in os.listdir(self._content):
            if name not in valid_ids:
                os.remove(os.path.join(self._content, name))
        _fsync_dir(self._content)

    # ---------------------------------------------------------------- 内部

    @staticmethod
    def _validate_request(
        project_id: str,
        session_id: str,
        idempotency_key: str,
        items: list[IngestItem],
    ) -> None:
        for label, value in (
            ("project_id", project_id),
            ("session_id", session_id),
            ("idempotency_key", idempotency_key),
        ):
            if not isinstance(value, str) or not value:
                raise ValidationError(f"{label} 必须是非空字符串")
        if not 1 <= len(items) <= MAX_FILES_PER_BATCH:
            raise ValidationError(
                f"每批文件数量须在 1 至 {MAX_FILES_PER_BATCH} 之间，实际 {len(items)}"
            )
        for item in items:
            if not isinstance(item.filename, str) or not item.filename:
                raise ValidationError("文件名必须是非空字符串")
            if not os.path.isfile(item.source_path):
                raise ValidationError(f"源文件不存在: {item.source_path}")
            size = os.path.getsize(item.source_path)
            if size > MAX_FILE_SIZE:
                raise ValidationError(
                    f"单文件超过 200 MiB 上限: {item.filename} ({size} 字节)"
                )

    @staticmethod
    def _copy_to_staging(
        source_path: str, staging_dir: str, seq: int
    ) -> tuple[str, int]:
        """流式复制并计算 SHA-256，返回 (hex digest, size)。"""
        digest = hashlib.sha256()
        size = 0
        dest = os.path.join(staging_dir, str(seq))
        with open(source_path, "rb") as src, open(dest, "wb") as dst:
            while True:
                chunk = src.read(COPY_CHUNK_SIZE)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_FILE_SIZE:
                    raise ValidationError("单文件超过 200 MiB 上限")
                digest.update(chunk)
                dst.write(chunk)
            dst.flush()
            os.fsync(dst.fileno())
        return digest.hexdigest(), size

    @staticmethod
    def _receipt_to_json(receipt: Receipt) -> str:
        return json.dumps(
            {
                "batch_id": receipt.batch_id,
                "project_id": receipt.project_id,
                "session_id": receipt.session_id,
                "idempotency_key": receipt.idempotency_key,
                "submitted_at": receipt.submitted_at,
                "files": [
                    {
                        "file_id": f.file_id,
                        "filename": f.filename,
                        "sha256": f.sha256,
                        "size": f.size,
                        "duplicate_warning": f.duplicate_warning,
                    }
                    for f in receipt.files
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
            files=tuple(
                FileReceipt(
                    file_id=f["file_id"],
                    filename=f["filename"],
                    sha256=f["sha256"],
                    size=f["size"],
                    duplicate_warning=bool(f["duplicate_warning"]),
                )
                for f in data["files"]
            ),
        )
