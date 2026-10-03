"""1 GiB 批次：收件峰值内存不超过 96 MiB，成功后删除源文件不影响读回。"""
import hashlib
import os
import resource
import sys

import pytest

from cad_inbox import BatchItemRequest, CadInbox

FILE_COUNT = 6
FILE_SIZE = 180 * 1024 * 1024  # 180 MiB，总量 1080 MiB > 1 GiB
MEMORY_LIMIT = 96 * 1024 * 1024


def _peak_rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux 单位为 KiB，macOS 单位为字节
    return rss if sys.platform == "darwin" else rss * 1024


def _make_large_source(path: str, size: int) -> str:
    """流式生成大文件（1 MiB 随机块重复），返回内容 SHA-256。"""
    chunk = os.urandom(1024 * 1024)
    digest = hashlib.sha256()
    remaining = size
    with open(path, "wb") as fh:
        while remaining > 0:
            block = chunk[: min(len(chunk), remaining)]
            fh.write(block)
            digest.update(block)
            remaining -= len(block)
    return digest.hexdigest()


@pytest.mark.slow
def test_one_gib_batch_memory_and_source_deletion(lib_dir, clock, tmp_path):
    box = CadInbox(lib_dir, now=clock)
    # 先有一个历史批次：历史文件增多不得影响单次收件内存
    small = tmp_path / "src" / "small.dwg"
    small.parent.mkdir(parents=True, exist_ok=True)
    small.write_bytes(b"history")
    box.submit_batch("p1", "s1", "k0", [BatchItemRequest("small.dwg", str(small))])

    sources, expected = [], []
    for i in range(FILE_COUNT):
        path = str(tmp_path / "src" / f"big{i}.dwg")
        expected.append(_make_large_source(path, FILE_SIZE))
        sources.append(path)

    reqs = [BatchItemRequest(f"big{i}.dwg", p) for i, p in enumerate(sources)]
    before = _peak_rss_bytes()
    receipt = box.submit_batch("p1", "s1", "k1", reqs)
    peak_delta = _peak_rss_bytes() - before
    assert peak_delta < MEMORY_LIMIT, f"峰值内存超出限制: {peak_delta / 2**20:.1f} MiB"

    # 成功返回后删除全部源文件
    for path in sources:
        os.remove(path)

    # 读回逐字节一致（流式校验，不把内容读入内存）
    assert len(receipt.items) == FILE_COUNT
    for item, want_sha in zip(receipt.items, expected):
        digest = hashlib.sha256()
        with box.open_file(item.file_id) as fh:
            while True:
                block = fh.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
        assert digest.hexdigest() == want_sha
        assert item.sha256 == want_sha
    box.close()
