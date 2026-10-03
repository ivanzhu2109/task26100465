# CAD 评审文件收件库

跨部门评审会的 CAD 文件收件库。文件内容保存在本地目录，元数据与收件回执保存在 SQLite。只提供三类能力：**批量收件、查询并读回会话文件、恢复**。

## 快速开始

```python
from cad_inbox import CadInbox, BatchItemRequest

with CadInbox("/var/lib/cad-inbox") as inbox:
    inbox.recover()  # 重新打开库后先恢复（幂等，可随时重复调用）

    receipt = inbox.submit_batch(
        project_id="proj-7",
        session_id="review-42",
        idempotency_key="upload-20261003-01",
        files=[
            BatchItemRequest("asm-01.dwg", "/exports/asm-01.dwg"),
            BatchItemRequest("asm-02.dwg", "/exports/asm-02.dwg"),
        ],
    )
    # 成功返回后，调用方可立即删除全部源文件

    for entry in inbox.list_session_files("proj-7", "review-42"):
        with inbox.open_file(entry.file_id) as fh:
            data = fh.read()  # 流式读回
```

## 语义

- **批次**：1–50 个文件，单文件 ≤ 200 MiB，整批原子进入会话；每项获得独立 `file_id`，内容相同也各自保留、分别读回。
- **幂等**：键按项目+会话隔离，自首次成功提交起 24 小时内，同键同顺序同文件名同内容返回原回执（不新增文件）；任一不同则拒绝且不影响已有结果。恰好满 24 小时时同键视为新批次，旧批次保留。源文件路径不属于请求身份。
- **重复警告**：每项计算 SHA-256，与同项目已提交文件或本批次更早项相同的，回执中 `duplicate=True`，不阻止收件；警告以首次提交为准，重试不会改写。
- **崩溃恢复**：提交点是 SQLite 事务。已返回的批次完整保留；未返回的批次恢复时完全撤回，不留半批文件。恢复不依赖源文件，可反复执行，只清理不属于已提交批次的内容文件。
- **内存**：全程 1 MiB 块流式处理，1 GiB 批次收件峰值内存增量约 6 MiB（限制 96 MiB）。

## 测试

```bash
python3 -m pytest          # 全部 33 个测试（含 1 GiB 内存检查）
python3 -m pytest -m "not slow"  # 跳过大数据量测试
```

测试通过 `crash_hook` 故障注入覆盖：收件各阶段中断、回执提交前后中断、恢复过程中再次中断、过期键复用中断等场景。
