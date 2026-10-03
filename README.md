# cad-inbox

CAD 评审文件收件库。文件内容保存在本地目录，元数据与收件回执保存在 SQLite，由上层直接调用。

## 能力

- **批量收件** `ingest(project_id, session_id, idempotency_key, items)`：一批 1–50 个文件，单文件 ≤ 200 MiB。整批作为整体进入会话，每项获得独立 `file_id`；内容相同的有意提交保留各自记录。返回 `Receipt` 即表示内容、元数据、回执均已提交，调用方可立即删除源文件。
- **查询与读回** `query_session(project_id, session_id)` / `open_file(file_id)` / `extract_file(file_id, dest)`：查询只返回完整批次；读回为流式，不依赖原始源文件。
- **恢复** `recover()`：进程在收件或恢复任意时刻被终止后调用。半成品批次全有或全无，清理无主内容文件，可反复执行。键过期不等于文件过期，已提交批次的内容永不清理。

## 规则要点

- 幂等键按 `(project_id, session_id)` 隔离，自首次成功提交起 24 小时有效；恰好满 24 小时同一键代表新批次，旧批次保留。
- 有效期内同键同顺序同文件名同内容返回原回执；任何一项不同则拒绝且不改变已有结果。源文件路径不属于请求身份。
- 每项计算 SHA-256；与同项目此前文件或批内更早项相同则带 `duplicate_warning`，不阻止收件。警告以首次提交时状态为准。
- 收件流式复制（1 MiB 块），1 GiB 批次实测峰值内存约 2.3 MiB（上限 96 MiB）。

## 用法

```python
from cad_inbox import CadInbox, IngestItem

with CadInbox("/var/lib/inbox") as inbox:
    inbox.recover()  # 重新打开后先恢复
    receipt = inbox.ingest(
        "proj-1", "session-9", "key-abc",
        [IngestItem("part-a.dwg", "/tmp/export/part-a.dwg")],
    )
    for batch in inbox.query_session("proj-1", "session-9"):
        ...
    with inbox.open_file(receipt.files[0].file_id) as f:
        data = f.read()
```

测试时可注入 `CadInbox(root, now=callable)` 控制时钟。

## 测试

```bash
python3 -m pytest tests/ -q
```

覆盖：收件中断、回执未返回、恢复中断、全有或全无、逐字节一致、重复恢复、重复警告、同键重试、24 小时边界、过期键复用中断、1 GiB 内存检查、删源读回。
