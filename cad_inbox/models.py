"""对外暴露的数据类型。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class IngestItem:
    """一次收件中的单个待收文件。"""

    filename: str
    source_path: str


@dataclass(frozen=True)
class FileReceipt:
    """单个文件的收件回执项。"""

    file_id: str
    filename: str
    sha256: str
    size: int
    duplicate_warning: bool


@dataclass(frozen=True)
class Receipt:
    """整批收件回执。"""

    batch_id: str
    project_id: str
    session_id: str
    idempotency_key: str
    submitted_at: float
    files: tuple[FileReceipt, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class FileView:
    """查询会话时返回的文件视图。"""

    file_id: str
    filename: str
    sha256: str
    size: int
    duplicate_warning: bool


@dataclass(frozen=True)
class BatchView:
    """查询会话时返回的批次视图（只含完整批次）。"""

    batch_id: str
    idempotency_key: str
    submitted_at: float
    files: tuple[FileView, ...] = field(default_factory=tuple)
