import hashlib
import os

import pytest

from cad_inbox import CadInbox, IngestItem


class FakeClock:
    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def root(tmp_path):
    return str(tmp_path / "inbox")


@pytest.fixture()
def src_dir(tmp_path):
    d = tmp_path / "sources"
    d.mkdir()
    return str(d)


@pytest.fixture()
def inbox(root, clock):
    box = CadInbox(root, now=clock)
    yield box
    box.close()


def make_source(src_dir: str, name: str, data: bytes) -> str:
    path = os.path.join(src_dir, name)
    with open(path, "wb") as f:
        f.write(data)
    return path


def make_items(src_dir: str, specs: list[tuple[str, bytes]]) -> list[IngestItem]:
    return [IngestItem(filename=n, source_path=make_source(src_dir, n, d)) for n, d in specs]


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_all(stream, chunk: int = 1 << 20) -> bytes:
    parts = []
    while True:
        block = stream.read(chunk)
        if not block:
            break
        parts.append(block)
    return b"".join(parts)
