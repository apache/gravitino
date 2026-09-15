# Review：6c2878fb1 — 跳过 EXPLICIT 修掉了双关，但换来一个漏关窗口

## 一、改动概述

`GravitinoCatalogManager` 的 client cache 原来在 removal listener 里无条件 `closeClient`，而 `close()` 已经先同步遍历关过一遍，于是每个 client 被关两次，第二次由 Caffeine 派发到 `ForkJoinPool.commonPool()`、在 `close()` 返回之后才跑。

改动分两部分：生产代码让 removal listener 跳过 `RemovalCause.EXPLICIT`（只有 `close()` 会产生这个 cause）；测试把 `ClientFactory` 的 per-client `AtomicBoolean` 换成 `AtomicInteger` 计数，`testCloseClosesEveryCachedClient` 在 `close()` 之后 drain common pool，再断言每个 client 只关一次。

方向是对的：双关确实该消掉，测试也确实能挡住回退。问题出在选的那个层次：guard 把「关不关」交给 cause 判断，于是 `close()` 里「先 drain 再 invalidateAll」的两步之间多了一个谁都不关的缝，详见条目 1。

## 二、Review 条目（按严重程度排序）

### 1. [MEDIUM · 正确性] drain 之后、invalidateAll 期间插进来的 client 再也没人关 (CONFIRMED)

- 锚点：`spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/catalog/GravitinoCatalogManager.java:102`
- 问题：guard 成立的前提是「`invalidateAll()` 能碰到的 entry，同步遍历那一行已经关过」，但没有任何代码保证这一点。`clients.asMap().forEach` 走的是 `Map.forEach` 默认实现，底层是 `ConcurrentHashMap` 的 `EntryIterator`，弱一致遍历；`invalidateAll()` → `BoundedLocalCache.clear()` 是另一次遍历，`removeNode` 把这类删除判成 `EXPLICIT`。`getClient` 里 `clients.get(identity, ...)` 的 `computeIfAbsent` 不持 evictionLock，也不看 `isClosed`，所以一个新 entry 完全可以在 `forEach` 的迭代器走过它所在的 bin 之后才发布，然后被 `clear()` 以 EXPLICIT 删掉，新 listener 直接 return，client 的 HTTP 连接池和 authDataProvider 就再没人关。改动之前这条路由 listener 兜底关掉。
- 失败场景：token 认证 + 共享 driver。查询线程走 `BaseCatalog` 构造 → `initialize()` → `getGravitinoCatalogInfo` → cache miss → `getClient(newIdentity)`，mapping function 里真建一个 `GravitinoClient`（含 HTTP 握手），占用毫秒级窗口；同时 `SparkContext.stop()` → `GravitinoDriverPlugin.shutdown()` → `close()` 正在同步遍历。只要新节点在遍历走过之后、`clear()` 到达之前发布，这个 client 就永久泄漏。窗口不是两行之间的几条指令，而是整个 `forEach` 的时长（N 次 `client.close()`），因为 `clear()` 是重新遍历一遍。`gravitinoCatalogManager = null` 在窗口之后，什么都挡不住。
- 处置：已修复（方案有调整：见「四、被否决的方案」）。cache 的 value 从 `GravitinoClient` 换成一层 `CachedClient`，`close()` 里 `closed.compareAndSet(false, true)` 成功才真的关。于是 listener 恢复成对所有 cause 无条件关闭，「只关一次」由 CAS 保证而不再依赖 cause 判断：同步遍历没看到、但 `invalidateAll()` 的遍历还看得到的 entry 由 listener 关掉（EXPLICIT 和 EXPIRED 都关），遍历已经关过的 entry listener 拿到时 CAS 失败、不会再关。还剩一个更窄的窗口没修：`BoundedLocalCache.clear()` 走的也是 `data.values()` 的弱一致遍历，发布得比这次遍历还晚的 entry 不会被 `removeNode` 碰到，listener 也就不触发，这个 client 仍然漏。它在改动前后一样存在，不是本 commit 引入的；要关掉得让 `getClient` 在 `isClosed` 之后不再往 cache 里放 client，那是另一件事。

### 2. [MEDIUM · 可维护性] guard 的前提和注释的保证代码都给不出，将来破了是一声不响地漏 (CONFIRMED)

- 锚点：`spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/catalog/GravitinoCatalogManager.java:156`
- 问题：注释第一句「Explicit removal comes only from close()」说的是调用点现状，不是代码性质。当时确实只有 `close()` 里那一次 `invalidateAll()` 会产生 EXPLICIT（全模块只有 `clients.get`、`asMap().forEach`、`invalidateAll()` 三处操作这个 cache），但这个前提由 private 可见性偶然维持，编译器和测试都不检查。同一段注释里「after close() has already returned」也不是保证：`notifyRemoval` 提交失败时会 inline 跑掉那个 task，parallelism 为 0 的 common pool 也会在调用线程上跑提交的任务。`close()` 上方新写的「no client outlives close()」同样偏强：`asMap().forEach` 的 `EntryIterator` 会跳过 `hasExpired` 为真但还没被物理清理的 entry，这些 entry 是随后 `invalidateAll()` 以 EXPIRED 触发 listener、在 common pool 上关掉的。
- 失败场景：将来有人为 token 轮换或鉴权失效加一句 `clients.invalidate(identity)`，那个 client 就永远不会被关，没有异常、没有日志、没有测试失败，只有一个活到进程结束的 HTTP 连接池。
- 处置：已修复。CAS 之后不再需要「EXPLICIT 只来自 close()」这个前提，任何 `clients.invalidate(key)` 都会正确关闭。两处注释重写成代码给得出的说法：listener 那段说明每次移除都关、CAS 保证不会关两次；`close()` 那段把保证限定成「同步遍历看到的 client 不会活过 close()」，遍历没看到的那些写成「`invalidateAll()` 还看得到就由 listener 关」，不再断言全都会被关。

### 3. [LOW · 测试] `testCloseClosesEveryCachedClient` 的断言组合挡不住回退 (CONFIRMED)

- 锚点：`spark-connector/spark-common/src/test/java/org/apache/gravitino/spark/connector/catalog/TestGravitinoCatalogManager.java:157`
- 问题：两处都偏弱。一是 `awaitQuiescence` 超时返回 `false` 没接，common pool 是 JVM 全局的，被别的任务占住 30 秒时 drain 没完成而断言照跑；二是 drain 之前那条 `closedCount() == 3` 的含义只是「三个 client 都至少关过一次」，一个纯异步的 listener 抢在断言之前跑完同样满足，钉不住「`close()` 返回时已经关了」。
- 失败场景：把 `close()` 里的同步遍历删掉、只留 listener，`closedCount() == 3` 变成抛硬币（实测异步 close 在 11ms 内就落地，异步路径能轻松胜出），drain 之后的 `[1, 1, 1]` 仍然通过，于是回退挡不住。
- 处置：已修复。drain 前后各断一次准确计数（`List.of(1, 1, 1)`），不去断 `awaitQuiescence` 的返回值，断它会引入共享 pool 带来的真 flaky。实测这组断言挡得住两种回退：删掉同步遍历只留 listener 得到 `[0, 0, 0]`，让 CAS 不再抑制第二次 close 得到 `[2, 1, 1]`。

## 三、被删掉的候选

- 「逻辑上已过期的 entry 同步遍历关不到、close 被丢到 common pool」原本单列一条 MEDIUM。异步这半改动前后一模一样，算在这个 commit 头上属于误归因；真正新增的只有注释那句过强的保证，已并入条目 2。
- 「drain 之前那条 `assertEquals(3, closedCount())` 证明不了在调用线程上关」原本单列一条 LOW。锚点行改动前就有，单独列属于扩大范围；它和 `awaitQuiescence` 那条由同一个改动一起解决，已并入条目 3。

## 四、被否决的方案

原计划是给 client cache 加 `Caffeine.executor(Runnable::run)`，让 listener 成为唯一的关闭路径并删掉 `close()` 里的同步遍历。Phase 2 的技术复评按 Caffeine 2.9.3 的字节码核过，这个方案不能用：`Caffeine#executor` 不只用于 removal notification，`scheduleDrainBuffers` 也用它提交 `PerformCleanupTask`，而且是持着 `evictionLock` 提交的。`clients.get(identity, ...)` 命中走 `afterRead`、未命中走 `afterWrite`，两条都会到 `scheduleDrainBuffers`，于是整个 maintenance（`drainReadBuffer` / `expireEntries` / `evictEntries`）连带所有被 size/TTL 淘汰的 client 的 `HTTPClient.close(GRACEFUL)` 都会同步跑在某个 Spark 查询线程上、且持着 evictionLock。TLS 连接的 `SSLSocket.close()` 要写 close_notify，对着一个卡住的 server 能阻塞，此时别的线程走到 `performCleanUp` 就堵在 evictionLock 后面。用户 A 的一次查询给用户 B..Z 刚过期的 client 付关连接的钱，这不是这个 commit 想换来的东西。

CAS 方案没有这个代价：executor 保持默认，淘汰路径仍然异步，只是「关一次」的保证从 cause 判断挪到了 client 自己身上。

## 五、结论

条目 1 是这次必须处理的：它把一个在生产上无害的双关（`GravitinoClientBase#close` 吞掉一切异常，HC5 的 close 幂等）换成了一个真的泄漏。复评认为它够不上 HIGH：泄漏的是一个 HTTP 连接池，而且发生在 SparkContext 正在停的 JVM 里，改动之前同一个竞争在一条指令之后也会漏；这个理由成立，所以定 MEDIUM 而不是 HIGH，但「原来一定会关、现在可能永远不关」仍然是回退，不是 LOW。

条目 1 和 2 同源，都来自「把关闭责任按 cause 分给两个地方」这个选择，一个 CAS 同时解决：不再需要 cause 判断，也不再需要那两句代码给不出的注释。条目 3 让测试真的钉住「`close()` 返回时已经关了」，而不只是「最终关了」。

Phase 3 复评：第一轮抓到一个必修项——条目 1 的 处置 和 `close()` 上方那句注释都把「遍历没看到的都会被关」写成了无条件的，而 `BoundedLocalCache.clear()` 自己也是弱一致遍历，比它还晚发布的 entry 谁都碰不到。改成有条件的说法后重新验证，第二轮只剩一条可选项（`CachedClient` 的 Javadoc 把 at-most-once 写成了 exactly once，已改），算一轮干净，达到 N=1。
