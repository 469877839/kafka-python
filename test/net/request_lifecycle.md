# 请求生命周期表（kafka-python 请求关联链路）

本表描述一个请求从 `KafkaConnection.send_request()` 到 future 终结的完整状态机，
以及各故障注入点对应的预期行为。每一行都由
`test/net/test_connection_fault_injection.py` 中对应用例钉住（不依赖真实网络，
事件次序由 `ScriptedTransport` 替身安排）。

约定：
- “在途队列”指 `conn.in_flight_requests`（`KafkaProtocol.in_flight_requests` 与之同步增减）。
- “完成次数”指 future 的回调触发次数；任何路径下都必须恰好为 1（或明确保持 pending）。
- 异常归属按连接隔离：某个节点的故障只能终结该连接自己的在途/缓冲 future，
  不得波及其他节点的连接（见 `test_failure_is_scoped_to_its_own_node`）。

| # | 阶段 / 故障注入点 | 触发方式 | future 状态 | 在途队列 | 连接状态 | 对应测试 |
|---|------------------|----------|-------------|----------|----------|----------|
| 1 | 初始化中发送 | `initializing=True` 时 `send_request` | pending，进入 `_request_buffer` | 不变 | initializing | `test/net/test_connection.py::TestKafkaConnectionSendRequest` |
| 2 | 正常发送 | `parser.send_request` 分配 correlation_id，写 transport | pending，入队在途 | +1 | connected | 各用例的 `Harness.send` |
| 3 | 长度前缀分片 | 4 字节前缀跨多次 `data_received` 到达 | 前缀未齐前保持 pending；齐后正常完成 | 完成前保持 1 | connected | `TestFragmentedLengthPrefix` |
| 4 | 响应粘包 | 多个完整帧一次 `data_received` 到达 | 按 FIFO 顺序各完成一次 | 逐帧 -1 至 0 | connected | `TestGluedResponses` |
| 5 | 未知 correlation_id | 无在途请求时收到响应帧 | 不归属任何 future；连接中止后该连接在途 future 以 `CorrelationIdError` 失败 | 清空 | closed | `TestUnknownCorrelationId` |
| 6 | 重复 correlation_id | 已完成的响应帧再次到达 | 原 future 不重复完成；当前在途 future 不被错配，随连接中止失败 | 清空 | closed | `TestDuplicateCorrelationId` |
| 7 | 错配 correlation_id（刻意交换） | 响应帧携带另一请求的 id | 任何 future 不得拿到错误响应；全部以 `CorrelationIdError` 失败一次 | 清空 | closed | `TestMismatchedCorrelationId` |
| 8 | 响应解析异常 | 帧长度/id 合法但 body 无法解码 | 不以成功完成；以 `KafkaProtocolError` 失败一次 | 清空 | closed | `TestResponseParseError` |
| 9 | 连接关闭 | `connection_lost(exc)` | 在途 + 缓冲 future 各以 `exc` 失败一次 | 清空 | closed，`close_future` 携带 `exc` | `TestConnectionCloseAndReconnect` |
| 10 | 关闭后重连 | 同节点新建连接 | 旧 future 保持已失败（不重复终结）；新请求在新连接上正常完成 | 旧 0 / 新独立 | 新连接 connected | `TestConnectionCloseAndReconnect` |
| 11 | 发送缓冲部分写失败 | `transport.write` 写了前缀后抛错 | **当前行为**：future 滞留在途（“一直不结束”症状），直到请求超时以 `RequestTimedOutError` 失败 | 超时前保持 1 | 超时前仍 connected，超时后 closed | `TestPartialWriteFailure` |
| 12 | 请求超时 | 响应迟迟不到，每请求定时器触发 | 以 `RequestTimedOutError` 失败一次 | 清空 | closed（超时主动 abort） | `TestRequestTimeout` |

## 关键不变量

- **恰好一次**：正常、重试前的失败、超时、连接关闭，任何路径下一个 future 只终结一次。
- **按序关联**：响应按在途队列 FIFO 与 correlation_id 双重核对；错配即协议错误，关闭连接。
- **故障隔离**：异常只清空本连接的在途/缓冲队列，不要求、也不允许清空其他节点的请求。
- **可恢复**：连接关闭后同节点可重建连接并继续完成新请求；旧连接的 future 不受新连接影响。
- **已知滞留点**（行 11）：`transport.write` 抛错时异常直接抛给 `send_request` 调用方，
  但 future 已入队在途且连接未关闭，只能等 `request_timeout_ms` 超时回收——
  这正是采集进程偶发“某个 future 一直不结束”的复现路径，默认 30s 超时下表现为长时间悬挂。
