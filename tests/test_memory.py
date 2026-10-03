"""总量 1 GiB 批次：收件期间进程峰值内存不超过 96 MiB，删源后仍可读回。"""

import hashlib
import os
import tracemalloc

from cad_inbox import IngestItem

PEAK_LIMIT = 96 * 1024 * 1024  # 96 MiB
TOTAL = 1024 ** 3  # 1 GiB
FILE_COUNT = 8
FILE_SIZE = TOTAL // FILE_COUNT  # 每个 128 MiB，低于 200 MiB 单文件上限
WRITE_CHUNK = 8 * 1024 * 1024


def _make_big_source(src_dir: str, name: str, size: int, seed: bytes) -> tuple[str, str]:
    """流式生成确定性内容的大文件，返回 (路径, sha256)。"""
    path = os.path.join(src_dir, name)
    digest = hashlib.sha256()
    block = (seed * (WRITE_CHUNK // len(seed) + 1))[:WRITE_CHUNK]
    remaining = size
    with open(path, "wb") as f:
        while remaining > 0:
            chunk = block[: min(remaining, len(block))]
            f.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
    return path, digest.hexdigest()


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(WRITE_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def test_one_gib_batch_memory_and_readback(inbox, src_dir, tmp_path):
    sources = []
    expected = {}
    for i in range(FILE_COUNT):
        path, sha = _make_big_source(src_dir, f"part{i}.dwg", FILE_SIZE, f"seed-{i}".encode())
        sources.append(path)
        expected[f"part{i}.dwg"] = sha

    items = [IngestItem(filename=os.path.basename(p), source_path=p) for p in sources]

    tracemalloc.start()
    try:
        receipt = inbox.ingest("proj", "sess", "big-batch", items)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < PEAK_LIMIT, f"峰值内存 {peak / 1024**2:.1f} MiB 超过 96 MiB"
    assert len(receipt.files) == FILE_COUNT
    for fr in receipt.files:
        assert fr.sha256 == expected[fr.filename]
        assert fr.size == FILE_SIZE

    # 成功返回后删除全部源文件，读回不受影响且逐字节一致。
    for p in sources:
        os.remove(p)

    for fr in receipt.files:
        dest = str(tmp_path / f"out-{fr.filename}")
        inbox.extract_file(fr.file_id, dest)
        assert _sha256_file(dest) == expected[fr.filename]
        os.remove(dest)

    # 恢复后内容文件数量与批次一致，无遗留。
    inbox.recover()
    assert len(os.listdir(os.path.join(inbox._content,))) == FILE_COUNT
