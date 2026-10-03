"""SQLite 元数据与回执存储层。"""

from __future__ import annotations

import sqlite3
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    submitted_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_key
    ON batches (project_id, session_id, idempotency_key, submitted_at);
CREATE INDEX IF NOT EXISTS idx_batches_session
    ON batches (project_id, session_id, submitted_at);

CREATE TABLE IF NOT EXISTS files (
    file_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES batches (batch_id),
    seq INTEGER NOT NULL,
    filename TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    duplicate_warning INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_batch ON files (batch_id, seq);
CREATE INDEX IF NOT EXISTS idx_files_sha ON files (sha256);
"""


class Storage:
    """封装 SQLite 连接与全部元数据读写。"""

    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def find_batch_by_key(
        self, project_id: str, session_id: str, idempotency_key: str
    ) -> dict[str, Any] | None:
        """返回该键最近一次提交的批次行，不存在则 None。"""
        row = self._conn.execute(
            """
            SELECT * FROM batches
            WHERE project_id = ? AND session_id = ? AND idempotency_key = ?
            ORDER BY submitted_at DESC, rowid DESC
            LIMIT 1
            """,
            (project_id, session_id, idempotency_key),
        ).fetchone()
        return dict(row) if row is not None else None

    def project_has_sha256(self, project_id: str, sha256: str) -> bool:
        """同项目此前已提交文件中是否存在该校验和。"""
        row = self._conn.execute(
            """
            SELECT 1 FROM files f
            JOIN batches b ON b.batch_id = f.batch_id
            WHERE b.project_id = ? AND f.sha256 = ?
            LIMIT 1
            """,
            (project_id, sha256),
        ).fetchone()
        return row is not None

    def commit_batch(
        self,
        batch: dict[str, Any],
        files: list[dict[str, Any]],
    ) -> None:
        """在单个事务中写入批次、文件与回执。"""
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO batches (
                    batch_id, project_id, session_id, idempotency_key,
                    fingerprint, receipt_json, submitted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch["batch_id"],
                    batch["project_id"],
                    batch["session_id"],
                    batch["idempotency_key"],
                    batch["fingerprint"],
                    batch["receipt_json"],
                    batch["submitted_at"],
                ),
            )
            self._conn.executemany(
                """
                INSERT INTO files (
                    file_id, batch_id, seq, filename, sha256, size,
                    duplicate_warning
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        f["file_id"],
                        batch["batch_id"],
                        f["seq"],
                        f["filename"],
                        f["sha256"],
                        f["size"],
                        1 if f["duplicate_warning"] else 0,
                    )
                    for f in files
                ],
            )

    def list_session_batches(
        self, project_id: str, session_id: str
    ) -> list[dict[str, Any]]:
        """按提交时间返回会话内全部批次及其文件。"""
        batches = [
            dict(r)
            for r in self._conn.execute(
                """
                SELECT * FROM batches
                WHERE project_id = ? AND session_id = ?
                ORDER BY submitted_at, rowid
                """,
                (project_id, session_id),
            )
        ]
        for batch in batches:
            batch["files"] = [
                dict(r)
                for r in self._conn.execute(
                    "SELECT * FROM files WHERE batch_id = ? ORDER BY seq",
                    (batch["batch_id"],),
                )
            ]
        return batches

    def get_file(self, file_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM files WHERE file_id = ?", (file_id,)
        ).fetchone()
        return dict(row) if row is not None else None

    def list_all_files(self) -> list[dict[str, Any]]:
        """返回全部已提交批次的文件行（供恢复使用）。"""
        return [dict(r) for r in self._conn.execute("SELECT * FROM files")]
