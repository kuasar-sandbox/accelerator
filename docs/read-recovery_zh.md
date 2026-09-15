[English](read-recovery.md) | [简体中文](read-recovery_zh.md)

# 读取错误与恢复

## 1. 概述

`accelerator` 通过 `fetch.Stream`、`sparse.Run`、Bundle 和 cache/Store client 提供单次不可变读取, 将业务重试和退避留给调用者. 运行中的沙箱由 `sandboxer` 同步保留必需 I/O, 直到成功或所有者结束操作/VM.

读取失败不会永久锁死 client、惰性索引或引用来源. 后续调用可以在原 endpoint 恢复. 该合同不授权重放写入或改变已选数据来源.

## 2. CLI

现有命令和参数不变. 直接 CLI 读取向命令所有者报告单次尝试错误. Store `Put`、Fill、ingestion、发布和快照捕获不由读取合同重放. 响应丢失不能证明写入未生效.

## 3. 配置

现有 cache/Store endpoint、pool 和单次操作 timeout 设置仍然有效. 即使未配置读取 deadline, 连接池拨号仍有有界连接超时. socket 和 RPC deadline 只约束单次尝试; 调用者的操作 context 决定是否还允许后续读取尝试.

不增加重试设置、fallback endpoint、健康屏蔽、服务重启或兼容模式. 连接池维护与业务读取保持独立.

## 4. 错误合同

`pkg/readerr.Mark(err, retryable)` 保留 `Error()` 和 `Unwrap()`, 仅增加 `Retryable() bool`. `Mark(nil, ...)` 返回 nil. 未标注错误不代表已确认永久失败. `IsPermanent` 遍历包装和 joined causes, 后发现的永久原因不会被暂时失败的同级原因遮蔽. 语义边界上的标记对其包装分支具有权威性.

正常对象结束 EOF 和已确认的 cache/Store miss 保留现有接口. 必需不可变对象查找将已确认缺失转为永久原因. 完整帧边界、已验证 Run 几何、不可变格式/范围违反及已确认的完整性/认证失败, 在掌握语义的位置标记. 网络 EOF、半帧/半流、后端内部取消及不透明访问错误保留实际原因. 自定义 decrypt/decode 错误不自动证明损坏; 内置格式验证和实际认证检查提供分类.

固定大小 ReaderAt 请求只执行一次尝试. short-nil 或已声明字段的真实截断是结构失败, 不能据此拼接另一个响应. 来源合同允许时, 完整读取的普通 EOF 仍然合法. 显式失败包装在 EOF 兼容处理之前保留. 随机访问元数据适配层阻止 `io.ReadFull` 吞掉完整 buffer 附带的源失败. 顺序来源保留单遍合同及诊断/错误链, 不增加重放行为.

## 5. 恢复与可靠性

连接额度包含空闲、借出和拨号中的连接. Acquire 和 refill 使用同一预留上限. 拨号失败释放预留后立即返回实际原因. 坏连接释放容量时即使没有归还健康连接, 也会唤醒等待者. 等待者在取消或取到健康连接后, 若仍有空余额度, 会转交它消耗的容量通知. 只有有界维护 worker 执行健康/refill 工作.

Close 阻止新发布、唤醒等待者并取消拨号; 借出连接仍由调用者持有至 Release. Release 与 Close 对发布互斥. socket 取消中断实际读写, 回调在连接归还池前停止或 join. 原 endpoint 恢复不依赖每次读前 Ping 或整池失效.

Cache Get/GetShard 和 Store Get 只执行一次业务尝试. 失败 payload 和 stream 被释放/取消. 后续调用重新读取完整对象, 不保留或拼接旧前缀. 合法服务端取消仍可重试; 已确认 miss 与无法联系服务端保持不同含义.

Bundle Open 保留原有元数据和来源选择规则. Chunk 索引准备串行加载, 在局部构建并验证 map, 只在成功时发布. 失败或取消不缓存. Reader Close 仍遵守现有 source lease 生命周期, 并阻止延迟发布. 每个已选 Manifest 层继续使用已选 Bundle 或 remote 来源; 本地读取失败不会转向另一 Bundle 或 Store. 有序 refs 不可用导致查找不确定时, remote miss 不会错误证明全局缺失, 原因链仍可检查.

并行范围读取在返回前 join 全部 worker, 包括取消路径. 原失败保留, 派生的同级取消不会替换它; 其他 worker 的永久原因保留在聚合中. 读取返回后, worker 不得继续改写调用者 buffer.

测试覆盖拨号失败、实际满池等待、取消消耗唤醒、refill/Close 并发、全池旧连接、原 endpoint 重启、半帧/半流清理、初始化恢复、来源隔离和聚合永久错误. 调用者的 CH/KVM 测试使用精确配套源集验证 Guest completion 和快照行为.

## 6. 性能

空闲 Acquire 路径使用现有连接 channel, 不取得发布 mutex. 失败尝试不创建无界后台 refill 任务. 停机期间连接额度和维护并发保持有界. 成功的 Bundle 准备长期复用, 失败准备只占用尝试局部状态. 现有流式稀疏遍历和 Hole/Zero/Data 语义不变.

## 7. See Also

- [Cache](cache_zh.md)
- [Store](store_zh.md)
- [Manifest](manifest_zh.md)
- [文件制品](file-artifacts_zh.md)
- [Sandboxer 同步读取恢复](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/read-recovery_zh.md)
- [提案与验收清单](https://github.com/kuasar-sandbox/sandboxer/issues/225)
