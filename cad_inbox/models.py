"""对外暴露的数据类型。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class BatchItemRequest:
    """批次中的一项：文件名 + 只读本地源文件路径。"""

    filename: str
    source_path: str


@dataclass(frozen=True)
class ReceiptItem:
    """回执中的一项：一项一个独立文件标识。"""

    file_id: str
    filename: str
    sha256: str
    size: int
    duplicate: bool


@dataclass(frozen=True)
class Receipt:
    """批次回执。成功返回即表示整批已独立保存。"""

    batch_id: str
    project_id: str
    session_id: str
    idempotency_key: str
    submitted_at: float
    items: Tuple[ReceiptItem, ...]


@dataclass(frozen=True)
class FileEntry:
    """查询会话时返回的文件条目（仅属于已提交批次）。"""

    file_id: str
    batch_id: str
    filename: str
    sha256: str
    size: int
    duplicate: bool
    submitted_at: float
