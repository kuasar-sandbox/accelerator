[English](prefetch.md) | [简体中文](prefetch_zh.md)

# Manifest prefetch 并发

`fetch.Prefetcher.Prefetch` 在按需读取之前,为已组合 Stream 预热 cache/store 对象。本页是该路径的准入与遍历契约。格式、解密和有界明文 chunk cache 仍见 [Manifest §4.8](manifest_zh.md#48-读路径细节)。缓存连接池仍见 [cache](cache_zh.md)。

## 1. 定位

Restore 和其他冷读通常会碰到大量物理 chunk。Prefetch 只对最终可见的 Manifest `ChunkRun` 发起完整对象 cache Get,使后续 UFFD fault 或 `ReadAt` 有机会命中已填充对象。完成只表示后端已接受或结束该 Get,不是驻留或就绪保证。

Prefetch 不校验、不解密、不 pin,也不填充 Stream 明文 cache。成功 Get 后立即 Release,不读取 Blob。

## 2. 为何要重叠 Get

单次 cache Get 通常很便宜。在同一 Fetcher 上串行等待下一次 Get 则不然:串行遍历会为每个可见 Data chunk 付出一次 unix-socket 或 RPC 往返。快照预热因此曾被该排队主导,而不是 GetCF 或 AES。

`prefetchStream` 现在把可见 Data chunk 交给大小为 `maxPrefetchGets` 的 worker pool。请求调度器同时准入同样数量的 prefetch Get。两个上限共用同一常量,遍历不能发起超过准入允许的 Get。

重叠往返可以缩短串行等待。这是本 Fetcher 的实现性质,不是平台 SLO。引用 restore 墙钟时间时须记录负载、cache 状态和修订;见[项目性能文档](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/perf_zh.md)。

## 3. 准入 — `requestScheduler`

一个 Fetcher 拥有一个调度器。由该 Fetcher 打开的所有 Stream 共用它。不同 Fetcher 相互独立。

Fetcher 在同一个底层 `cache.Getter` 上暴露两个逻辑 Getter:

| Getter | 使用者 | 准入 |
| --- | --- | --- |
| On-demand | Manifest metadata、`Stream.ReadAt`、`Run.ReadAt` | 立即开始。从不等待 prefetch。 |
| Prefetch | `Prefetcher.Prefetch` | 等到 `onDemand == 0` 且在途 prefetch Get 少于 `maxPrefetchGets`。 |

`maxPrefetchGets` 为 **8**。它是内部常量,不是 YAML 或 CLI 开关。该值低于典型 sandboxer `cache.pool` 的 **16**,以便 prefetch 进行时 on-demand UFFD fault 仍能 `Acquire` 一条 cache 连接。

规则:

- 存在任意在途 on-demand Get 时,不再准入新的 prefetch Get。
- 已进入 inner Getter 的 prefetch Get 不会因 on-demand 开始而被取消。二者可短暂重叠。
- 在仍有 on-demand 时结束一个 prefetch Get,不会唤醒另一个 prefetch。
- 在进入 inner Get 前胜出的取消不会泄漏 prefetch slot。
- 调度器不创建后台 goroutine,也不拥有底层 Getter 的生命周期。

## 4. 遍历 — `prefetchStream`

`manifestStream.Prefetch` 与 `layeredStream.Prefetch` 都调用 `prefetchStream`。

遍历按逻辑 offset 单线程进行。Hole、Zero,以及不是包内 `prefetchChunkRun` 的 Data,都是 no-op。每个合格 Data run 是一个 job:`ChunkRun.prefetch` → `loadChunkAt(..., loadPrefetch)` → prefetch Getter Get → Release。

八个 worker 拉取 job。job channel 满时会反压遍历,因此一次 `Prefetch` 在途的 chunk prefetch 不超过八个。第一个错误取消 walk context;返回前会 join 已启动的 job。同一 Stream 上并发的 `Prefetch` 调用保持独立:不得共享同一个 Blob handle。

Prefetch 作用于当前组合 Stream 的完整可见视图,不再提供按 manifest key 选择叶子的接口。

## 5. 兼容性

- 公共 `Stream` 与 `Prefetcher` API 不变。
- 失败的 prefetch 不得污染后续 `ReadAt`。
- `sandboxer` 经 `replace => ../accelerator` 消费本 module。只有针对本树重建 `sandbox-ctl` 后,restore 才会看到新的准入。
- 若部署的 cache Get pool 小于 16,依赖 UFFD fault 余量为前提时须先在源码中降低 `maxPrefetchGets`。不要把 8 当作对任意更小 pool 都安全的值。

## 6. See also

- [Manifest §4.8](manifest_zh.md#48-读路径细节):Run/Stream 读路径、明文 cache,以及只取密文的 prefetch。
- [cache](cache_zh.md):cache Get 协议与连接池。
- [sandboxer](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md):消费 Manifest fetch 的快照 restore。
