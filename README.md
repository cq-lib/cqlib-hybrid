# cqlib-hybrid

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Python SDK for classical–quantum hybrid tasks on the Tianyan cloud. Apache-2.0. Python 3.9+.

- **Inside a QRMI container** (`cqlib_hybrid.QRMIClient`): submit QCIS, receive results, and report progress over the platform WebSocket. The only runtime dependency is `websocket-client`.
- **On the submit side** (`cqlib_hybrid.fusion`, `pip install "cqlib-hybrid[fusion]"`): validate and build a task package, run it locally, ask the cloud for a schedule, submit, and download results. The `cqlib-hybrid` command provides the same flow.

```bash
python3 -m pip install cqlib-hybrid                 # container client
python3 -m pip install "cqlib-hybrid[fusion]"       # task packages, local runs, cloud client
python3 -m pip install "cqlib-hybrid[fusion,quantum]"  # also run quantum stages on Tianyan; needs Python >= 3.10
```

Pass credentials through `TIANYAN_API_KEY` (or `CQLIB_HYBRID_TOKEN`) rather than command-line arguments, so they do not show up in the process list. The client does not print keys or tokens. Load only task packages you trust: running one executes its Python code. A cloud package must contain `tianyan.env` at the ZIP root with `TIANYAN_APP_SHOTS`, `TIANYAN_APP_ITERATIONS` and `TIANYAN_APP_WAIT_TIMEOUT`. `get_job()` returns task metadata; the live status comes from `progress()` or `wait_job()`.

Source: <https://github.com/cq-lib/cqlib-hybrid>

---

`cqlib-hybrid` 是天衍经典—量子融合（超量融合）任务的 Python SDK，覆盖任务链路的两端：

- **容器端**（`cqlib_hybrid.QRMIClient`）：在 QRMI 平台启动的容器内，通过 WebSocket 长连接提交 QCIS 量子线路、接收计算结果，并上报任务进度和最终状态。算法自行安排经典计算与量子调用。
- **提交端**（`cqlib_hybrid.fusion`，需安装 `fusion` 额外依赖）：任务包的校验与构建、本地运行、云端调度推荐与提交、状态查询和结果下载，以及 `cqlib-hybrid` 命令行。

SDK 支持 Python 3.9+，核心依赖只有 `websocket-client`，容器镜像无需安装其他包。安装包名为 `cqlib-hybrid`，Python 导入名为 `cqlib_hybrid`。

## 安装

在项目根目录执行：

```bash
python3 -m pip install .                 # 容器端：QRMIClient
python3 -m pip install ".[fusion]"       # 提交端：任务包、本地运行、云端客户端、CLI
python3 -m pip install ".[fusion,quantum]"  # 另需在天衍真机上运行量子阶段（cqlib-tianyan，需要 Python >= 3.10）
```

## QRMI 容器长连接

`QRMIClient` 用于已经由 QRMI 平台启动的容器。平台必须注入 `QRMI_HYBRID_CPU_CONNECTION_URL`、当前作业票据和心跳间隔；SDK 不内置部署地址。公开服务应使用 `wss://` 加密连接；显式配置的 `ws://` 仅适用于已由部署方保护的网络，它会明文传输票据和业务数据。

容器 SDK 使用原生 WebSocket 和 `Authorization` 作业票据认证，默认不发送浏览器 `Origin` 请求头，以保持命令行客户端调用一致。

| 环境变量 | 用途 |
| --- | --- |
| `QRMI_HYBRID_CPU_CONNECTION_URL` | 必填，完整 `wss://` 地址（受保护网络可显式使用 `ws://`） |
| `QRMI_HYBRID_CPU_TICKET` | 必填，当前作业票据，用于 WebSocket 握手的 `Authorization: Bearer ...` |
| `QRMI_HYBRID_CPU_HEARTBEAT_SECONDS` | 必填，正整数心跳间隔，单位秒 |

```python
from cqlib_hybrid import QRMIClient


with QRMIClient.from_env() as client:
    # 进入上下文时连接并等待 STARTED 确认。
    result = client.run_quantum("H Q0\nM Q0", shots=1000, timeout=600)
    client.progress(100)
    client.complete({"quantum_result": result})
```

客户端在后台持续接收消息并按配置间隔发送 `PROGRESS`，因此经典计算和等待量子结果期间都能保持心跳。`progress(30)` 让后续心跳携带进度百分比；它不会同步等待一次心跳确认。`with` 退出时只关闭连接，最终成功或失败必须显式调用 `complete()` 或 `fail()`。

## 量子调用

`run_quantum()` 自动按从 1 开始的连续轮次提交 QCIS 线路，等待 `QPU_RESULT` 后返回 `result` 值；结果可以是任意合法 JSON 值，包括对象、数组、字符串、数字、布尔值或 `None`，一般保持原值；当成功结果包含字符串 `experimentResults`，并带有 `waveforms` 或 `measureQubitList` 时，SDK 会解码 Base64 测量数据，添加 `counts` 和 `shots`，同时保留原字段。上一轮成功后才能开始下一轮。失败或取消状态会抛出 `QRMIQuantumError`，可读取其 `iteration_no`、`task_status` 和 `result` 属性。可选参数 `topology_mapping_enabled=True/False` 控制拓扑映射；省略时由平台决定。

需要分别提交和等待时，可以使用以下接口；`wait_quantum_result()` 返回完整推送消息，失败、取消也作为消息返回：

```python
iteration = client.submit_quantum("H Q0\nM Q0", shots=1000)
message = client.wait_quantum_result(iteration, timeout=600)
if message["taskStatus"] == 3:
    result = message["result"]
else:
    client.fail("量子计算失败或取消")
```

## 重试、超时与恢复

`QUANTUM_SUBMIT` 的确认只表示请求被接受，计算成功取决于后续 `QPU_RESULT`。客户端关联轮次并去重结果；请求被标记为可重试时重放相同内容，断线后用原票据重新连接并先发送 `STARTED`，已确认提交且尚未收到结果的轮次随启动消息恢复。量子提交或完成确认丢失时，按 `retry_interval` 原样重发同轮线路或最终结果；连接中断时也会在启动确认后重放，不重新执行算法。所有重试均受请求确认总时限约束。

可用 `QRMIClient.from_env(request_timeout=30, connect_timeout=10, retry_interval=1, max_reconnect_attempts=3)` 配置请求确认等待时间、连接超时、重试间隔和自动重连次数上限；时间单位为秒，自动重连次数是整个客户端实例的累计上限。`wait_quantum_result()` 和 `run_quantum()` 的 `timeout` 控制结果等待时间，`None` 表示无限等待。结果等待超时仅结束本次等待，不会取消服务端量子任务；请求确认超时则关闭客户端，此时服务端仍可能已经接受请求。票据失效、任务取消或执行身份失效需要由部署方处理，心跳不能延长票据有效期。

进程恢复时，若已知正在等待的轮次，可显式连接后接收其结果：

```python
client = QRMIClient.from_env()
try:
    client.connect(iteration_no=2)
    message = client.wait_quantum_result(2, timeout=600)
    # 根据 taskStatus 恢复算法状态，再继续迭代或显式上报终态。
finally:
    client.close()
```

该方式只能恢复指定轮次的等待，客户端无法重建未知的原始线路或算法内存状态。同轮重发必须与首次提交内容相同，恢复所需的算法状态应由调用方保存。

## 最终状态与数据限制

`complete(result, stdout=None, stderr=None, exit_code=0)` 和 `fail(error_message, error_code="ALGORITHM_FAILED", exit_code=1, stdout=None, stderr=None)` 都等待服务端确认后关闭连接。调用 `complete()` 前不得存在未结束的量子请求，已有的最后一轮必须成功。`FAILED` 收到明确的 `retryable=true` 拒绝响应时可以重试；已发送后断线丢失确认时抛出连接错误，因为文档没有保证该终态请求可在确认丢失时安全重放。客户端检查 JSON 类型、轮次和大小限制：单条消息最多 3 MiB，线路或结果等业务内容（含必要 JSON 结构）最多 1 MiB，每个日志字段的 UTF-8 内容最多 1 MiB。

结果仅支持标准 JSON 类型，拒绝 NaN、Infinity、循环引用、非字符串字典键及其他非 JSON 对象。NumPy 数据等应先转换为 Python 标量或列表。`fail()` 不直接退出 Python 进程，调用方应在上报确认后抛出原异常或以非零状态退出。

## 错误处理

所有 SDK 异常继承 `HybridError`，可从 `cqlib_hybrid` 导入。

| 异常 | 含义 |
| --- | --- |
| `HybridValidationError` | 本地参数、JSON 内容、轮次或消息大小不合法 |
| `HybridRuntimeError` | 缺少必填的 QRMI 环境变量，也是 QRMI 运行时异常的基类 |
| `HybridRequestError` | 握手请求异常的基类，提供可选的 `status_code` |
| `HybridAuthenticationError` | WebSocket 握手认证失败，保留 `status_code` |
| `HybridProtocolError` | 服务端响应格式不合法 |
| `QRMIRequestError` | 服务端拒绝请求，保留 `event_type`、`iteration_no`、`retryable`、`error_code` 和 `error_message` |
| `QRMIQuantumError` | 量子任务失败或取消，保留 `iteration_no`、`task_status` 和 `result` |
| `QRMIConnectionError` | 连接不可用、握手失败或连接恢复失败 |
| `QRMITimeoutError` | 请求确认或量子结果等待超时，同时继承 `TimeoutError` |

## 完整示例

完整示例 [examples/programs/qrmi_job.py](examples/programs/qrmi_job.py) 执行两轮量子调用、上报进度和最终结果，并在算法异常时报告失败。运行容器须安装本 SDK；该示例仅使用本 SDK，通过 WebSocket 收发数据。

```bash
python3 examples/programs/qrmi_job.py
```

## 项目结构

源码位于 `src/cqlib_hybrid/`。

| 路径 | 职责 |
| --- | --- |
| `src/cqlib_hybrid/__init__.py` | 导出 QRMI 客户端和异常类型 |
| `src/cqlib_hybrid/qrmi.py` | 长连接、心跳、量子调用、重试及最终状态上报 |
| `src/cqlib_hybrid/qrmi_protocol.py` | JSON 消息构造、响应解析及字段、类型和大小校验 |
| `src/cqlib_hybrid/exceptions.py` | QRMI 客户端异常类型 |
| `src/cqlib_hybrid/fusion/` | 超量融合：任务包、工作流、运行时、云端客户端、CLI |
| `examples/programs/qrmi_job.py` | 两轮量子调用的容器示例 |
| `examples/fusion/` | 四种模式的融合任务示例和云端提交示例 |
| `tests/` | 协议、客户端、示例和本地 WebSocket 集成测试；`tests/fusion/` 为融合部分 |

## 超量融合任务（`cqlib_hybrid.fusion`）

提交端功能为纯 Python 实现，依赖 `pip install ".[fusion]"`（`pyyaml`、`requests`）。

### 命令行

```bash
cqlib-hybrid validate examples/fusion/iterative          # 校验任务目录
cqlib-hybrid build examples/fusion/iterative -o task.zip # 打包
cqlib-hybrid run-local examples/fusion/iterative         # 本地运行（默认 mock 量子后端）

# 云端：密钥放环境变量，不要写进命令行。TIANYAN_API_KEY（或 CQLIB_HYBRID_TOKEN）
cqlib-hybrid submit task.zip --name demo                 # 先给出 TOP 3 推荐，未选择不会创建任务
cqlib-hybrid submit task.zip --name demo --plan GROUP_ID --wait
cqlib-hybrid submit task.zip --name demo --auto-confirm
cqlib-hybrid submit task.zip --name demo --dispatch manual \
  --quantum-resource tianyan176 --hpc-resource cloud-hpc
cqlib-hybrid status TASK_ID
cqlib-hybrid download TASK_ID ./results
```

环境变量：`TIANYAN_API_KEY`（也识别 `CQFUSION_LOGIN_KEY`、`LOGIN_KEY`）、`CQLIB_HYBRID_HOST`（默认 `https://qc.zdxlz.com`）、`CQLIB_HYBRID_TOKEN`。客户端不会打印密钥或令牌。只运行你信任的任务包，本地运行会执行包里的 Python。

提交到云端的 ZIP 根目录必须有 `tianyan.env`，并且定义 `TIANYAN_APP_SHOTS`、`TIANYAN_APP_ITERATIONS`、`TIANYAN_APP_WAIT_TIMEOUT`，否则平台会拒绝上传。`get_job()` 只返回任务的基础信息，不含运行状态和迭代数；状态用 `progress()` 或 `wait_job()`。

本地运行默认使用独立临时工作目录。需要保留断点时，请指定 `--workdir ./run-demo`；失败后使用相同任务和 `--workdir ./run-demo --resume` 显式恢复。Python API 对应 `Orchestrator(..., resume=True)`。恢复会校验工作流、任务文件和 manifest；旧版断点或不匹配的任务会被拒绝，应使用新的工作目录。工作目录中的日志和断点由调用方管理；ZIP 解包目录在本地运行退出后自动清理。

任务构建和静态校验共用文件筛选规则，跳过虚拟环境、缓存、构建目录、符号链接及常见凭据文件（例如 `.env.*`、`*.pem`、`*.key`）。构建结果还会执行上传格式校验：根目录恰好一个 `main.py`，整个包恰好一个 `.qcis`。上传前应检查 ZIP 内容，自定义文件名中的凭据仍需自行移除。HTTP 登录、上传和下载均不跟随重定向；请配置最终服务地址。

### Python API

```python
from cqlib_hybrid.fusion.client import FusionClient

client = FusionClient()                     # 首次请求时用 TIANYAN_API_KEY 登录
schedule = client.schedule_package("task.zip", strategy="balanced")
print(schedule.candidates())                # TOP 3 推荐，含资源编码与是否可选
handle = client.submit_decision("demo", schedule.select(1))   # 或 schedule.manual(qpu, cpu)
job = handle.wait(timeout=3600)
handle.download("results")
```

`FusionClient` 还提供 `list_jobs`、`presets`、`progress`、`get_job`、`wait_job`、`delete_job`、`download_job_result`、`download_job_log` 和兼容旧选项形式的 `submit_job`。提交、调度失败抛出 `CqfusionRequestError`，保留 `status_code`、`code`、`kind`、`data`。只读请求遇瞬时故障最多重试两次；写请求在超时、断连、429、5xx 后绝不重放，401 仅刷新一次令牌。

### 在容器里运行同一份工作流

量子后端可选 `mock`、`tianyan` 和 `qrmi`。`QRMIQuantumRunner` 通过容器内的 `QRMIClient` 提交线路，使同一份 `FusionWorkflow` 既能本地调试，也能在 QRMI 容器内运行：

```python
from cqlib_hybrid.fusion.runtime.orchestrator import Orchestrator
from cqlib_hybrid.fusion.runtime.quantum import QRMIQuantumRunner

runner = QRMIQuantumRunner.from_env()       # 读取 QRMI_HYBRID_CPU_* 环境变量并连接
```

## 验证

安装 SDK 后，在项目根目录执行：

```bash
python3 -m unittest discover -s tests -v    # QRMI 部分
python3 -m pip install ".[dev]" && python3 -m pytest   # 全部，含 fusion
```

测试使用模拟 WebSocket 响应和本地回环服务验证协议、连接、心跳、量子调用、重试与异常处理，不访问生产服务或消耗 QPU 额度。运行集成测试需要允许绑定本机临时端口。真实部署还需验证 QRMI 票据、长连接配置及量子设备权限。
