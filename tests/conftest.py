import hashlib
import os

import pytest

from cad_inbox import CadInbox


class FakeClock:
    def __init__(self, start: float = 1_700_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def lib_dir(tmp_path):
    return str(tmp_path / "lib")


@pytest.fixture
def inbox(lib_dir, clock):
    box = CadInbox(lib_dir, now=clock)
    yield box
    box.close()


def make_source(tmp_path, name: str, data: bytes) -> str:
    """在 tmp_path 下创建源文件，返回路径。"""
    path = tmp_path / "src" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return str(path)


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_all(fh, chunk: int = 1024 * 1024) -> bytes:
    parts = []
    while True:
        block = fh.read(chunk)
        if not block:
            break
        parts.append(block)
    return b"".join(parts)


def content_dirs(lib_dir: str):
    """返回 (staging 下的目录名, files 下的目录名)。"""
    staging = os.path.join(lib_dir, "staging")
    files = os.path.join(lib_dir, "files")
    st = sorted(os.listdir(staging)) if os.path.isdir(staging) else []
    fl = sorted(os.listdir(files)) if os.path.isdir(files) else []
    return st, fl
