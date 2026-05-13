# Lance Catalog-Aware Table Commits 调研报告

## 执行摘要

Lance 当前更自然的表提交模式是 `Storage-managed`：客户端直接修改底层存储，并通过写入新的 manifest 形成新版本。这种方式实现简单，也保留了 Lance 依赖递增版本号、`_versions` 和 manifest 命名规则构建的自描述版本语义；但问题在于，Catalog 不一定能稳定感知每一次提交，因此在权限前置、审计、血缘和下游触发方面存在明显治理缺口。

围绕这一问题，公开讨论可以归纳为五类方案：`Storage-managed`、`Storage-managed + Catalog Notification`、`Implementation-managed`、`CommitTable / Catalog-Aware Commit`、`ManifestStore / Catalog Intercept Commit`。其中，`Implementation-managed` 虽然治理能力强，但过于依赖 Catalog latest pointer，会削弱 Lance 原有的版本发现和分支管理语义；`Storage-managed + Notification` 只能提供事后补偿式治理，不适合作为最终提交架构。

从当前公开讨论的演进看，更合理的方向不是让 Catalog 接管 latest pointer，而是在保留 Lance 递增版本号、`_versions` 和原生冲突解决模型的前提下，让 Catalog 参与提交入口或版本协调。基于这一判断，本文最终推荐将 **ManifestStore / Catalog Intercept Commit** 作为 Lance 的长期方向，将 **CommitTable** 视为可以接受但更适合作为上层统一提交接口的方案。

简言之，本文的核心结论是：**Lance 更适合采用“Catalog 参与版本协调，而非 Catalog 接管 latest pointer”的路线。**

## 1. 文档目的

本文基于 Lance 社区关于 Catalog-Aware Table Commits 的公开讨论，对 Lance 表提交机制与 Catalog 治理之间的关系进行梳理，并比较几种可能的实现方案。

本文重点回答以下问题：

1. Lance 当前如何提交表版本。
2. 引入 Catalog 后，提交路径为何会成为关键设计点。
3. `Storage-managed`、`Storage-managed + Notification`、`Implementation-managed`、`CommitTable`、`ManifestStore / Catalog Intercept` 五类方案分别意味着什么。
4. 在当前公开讨论基础上，哪个方向更适合作为 Lance 的长期方案。

说明：本文中的部分方案命名与分层属于工程分析抽象，不完全等同于 Lance 社区已经定稿的正式规范。

------

## 2. 背景

Lance 当前更自然的使用方式，是由上层引擎或 Lance SDK 直接修改底层存储中的 Lance 表，并通过写入新的 manifest 文件形成新版本。典型流程如下：

```text
Engine / Lance SDK
  ↓
读取当前最新 manifest
  ↓
写入数据文件、删除文件或索引文件
  ↓
生成 transaction
  ↓
生成新的 manifest
  ↓
提交到 _versions 目录
  ↓
形成新的表版本
```

在这一模式下，表的最新版本主要由 Lance 自身的 `_versions` 目录和 manifest 命名规则决定，而不是由 Catalog 中的 latest pointer 决定。

这一设计保留了 Lance 作为开放表格式的自描述能力，但也带来一个直接问题：如果提交发生在存储层，Catalog 不一定能够稳定感知每一次表变更。Lance discussion #5229 的核心，就是在讨论如何让 Catalog 参与提交，同时尽量保留 Lance 原有的版本管理语义。

------

## 3. Lance 当前提交机制概述

Lance 通过 manifest 管理表版本。每次 append、delete、update、create index 等操作，都会基于当前版本生成 transaction，再据此生成新的 manifest。

可将其简化理解为：

```text
旧版本 manifest + transaction → 新版本 manifest
```

例如：

```text
_versions/
  1.manifest
  2.manifest
  3.manifest
```

此时 `3.manifest` 是当前最新版本；提交新版本后，目录可能变为：

```text
_versions/
  1.manifest
  2.manifest
  3.manifest
  4.manifest
```

此时 `4.manifest` 成为最新版本。

这里的“生成 transaction”，本质上是将本次变更整理成一份结构化提交对象，其中包含读到的基线版本、操作类型，以及新增、删除、重写的元数据；随后再据此生成新 manifest 并尝试提交。

Lance 依赖底层存储提供的原子能力保证并发提交安全，例如：

```text
put-if-not-exists
rename-if-not-exists
```

也就是说，多个 writer 并发提交同一版本号时，只有一个能够成功。

------

## 4. 引入 Catalog 后的关键问题

Storage-managed 模式实现简单，但在引入 Catalog 治理后，会暴露出四类问题：

### 4.1 Catalog 可见性不足

如果某个任务直接写入：

```text
_versions/10.manifest
```

那么 Lance 表已经更新到 `v10`，但 Catalog 可能并不知道这次提交已经发生。

### 4.2 权限控制无法前置

如果 commit 直接发生在存储层，Catalog 无法在提交前统一判断：

```text
用户是否有写权限
schema 变更是否合法
是否允许删除数据
是否符合治理策略
```

因此，Catalog 很难成为真正的提交控制点。

### 4.3 审计与血缘不完整

如果提交没有经过 Catalog，Catalog 无法稳定记录：

```text
谁提交的
什么时候提交的
提交了什么操作
影响了哪些数据
产生了哪个新版本
```

这会削弱审计、责任归属和数据血缘能力。

### 4.4 下游触发不可靠

很多治理动作依赖于“表已经发生变更”这一事实，例如：

```text
索引刷新
缓存失效
质量检查
血缘更新
训练任务触发
```

如果 Catalog 未感知 commit，这些下游流程就可能漏触发或晚触发。

------

## 5. 方案分类

结合 Lance 公开讨论与工程实现视角，本文将相关方案归纳为五类：

```text
方案一：Storage-managed table
方案二：Storage-managed + Catalog Notification
方案三：Implementation-managed table
方案四：CommitTable / Catalog-Aware Commit
方案五：ManifestStore / Catalog Intercept Commit
```

其中，前四类主要对应表提交接口设计，方案五更偏底层版本协调机制设计。

------

## 6. 方案一：Storage-managed table

`Storage-managed` 是 Lance 当前最自然的模式：客户端直接修改 Lance 表，并向 `_versions` 目录提交新的 manifest。

```text
Engine / Lance SDK
  ↓
直接修改 Lance 表
  ↓
提交新的 manifest 到 _versions
```

在这一模式下，最新版本由 Lance 自身存储结构决定，而不是由 Catalog 决定。

优点：

```text
实现简单
不强依赖 Catalog
保留 Lance 自描述能力
适合轻量和实验场景
```

缺点：

```text
Catalog 不一定感知 commit
权限控制无法前置
审计和血缘可能缺失
下游触发不可靠
```

该模式适合单团队、轻治理、低审计要求的场景，不适合作为强治理平台的最终方案。

------

## 7. 方案二：Storage-managed + Catalog Notification

这是对 Storage-managed 的增强：引擎先直接提交 Lance 表，再在提交成功后通知 Catalog。

```text
Engine / Lance SDK
  ↓
直接提交 Lance 表
  ↓
提交成功后通知 Catalog
  ↓
Catalog 记录版本变化、审计和血缘
```

该模式可以让 Catalog 事后感知表变化，并补充记录：

```text
table_id
old_version
new_version
operation_type
operator
job_id
timestamp
manifest_location
```

也可以进一步触发审计、血缘更新、缓存刷新和下游事件。

但其本质问题没有改变：Catalog 仍然是事后观察者，而不是提交控制者。因此仍然存在：

```text
忘记通知
通知失败
重复通知
状态滞后
无法阻止非法提交
```

该模式适合作为过渡方案或补充治理机制，但不适合作为最终提交架构。

------

## 8. 方案三：Implementation-managed table

`Implementation-managed` 更接近 Iceberg 模式，其核心是：由 Catalog / Namespace 决定如何提交，以及哪个 manifest 才是当前最新版本。

例如，Catalog 中可能维护：

```text
latest_manifest_location = s3://bucket/table/_versions/10.manifest
```

在这种模式下，最新版本不再只由 `_versions` 目录决定，而是由 Catalog latest pointer 决定。

可以用一个最小例子理解这一点。假设有一张表 `image_embeddings`，当前存储中已有：

```text
_versions/
  9.manifest
  10.manifest
```

同时 Catalog 中记录：

```text
latest_manifest_location = .../10.manifest
```

现在有一个 writer 要 append 一批新数据。在该模式下，writer 不再直接把 `11.manifest` 写入后就视为提交完成，而是先把提交请求交给 Catalog。随后由 Catalog：

```text
1. 读取自己记录的 latest pointer
2. 基于 10.manifest 组织本次提交
3. 生成或接收新的 11.manifest
4. 写入底层存储
5. 将 latest_manifest_location 更新为 11.manifest
```

也就是说，在方案三中，“哪个版本是当前最新版本”不再仅仅取决于存储中是否存在 `11.manifest`，而取决于 Catalog 是否已经将 latest pointer 切换到该版本。

这里还需要注意，`11.manifest` 不一定必须由 Catalog 亲手生成。更一般地说，Catalog 可以自己生成新的 manifest，也可以接收 client / engine 预先生成的候选 manifest；但无论哪种方式，**最终是否采用该 manifest、是否将其切换为当前最新版本，都由 Catalog 决定**。

这一模式的优势在于控制范围最大、治理能力也最强：

```text
Catalog 完整可见
权限控制集中
审计能力强
下游触发可靠
同时掌握最新版本认定权
```

但问题也很明显：

1. 削弱 Lance 的自描述能力。读取最新版本不再仅依赖 `_versions`。
2. 与 branching 配合复杂。原本每个 branch 可以在自己的存储位置下按 manifest 规则解析最新版本；改成 latest pointer 后，Catalog 需要显式管理每个 branch 的当前指向、merge、rollback 和冲突处理。
3. 可能形成两套 truth。存储结构认为 `main` 最新是 `v10`，Catalog pointer 可能认为最新是 `v9`。

更准确地说，方案三、方案四、方案五都属于强治理方案；区别在于，方案三不仅控制提交入口，还控制“谁是最新版本”，因此控制范围最大，但对 Lance 原生版本语义侵入也最大。

因此，该模式虽然治理强，但对 Lance 原生版本语义侵入较大，不建议作为 Lance 的长期方向。

------

## 9. 方案四：CommitTable / Catalog-Aware Commit

### 9.1 基本思路

`CommitTable` 可以理解为一个新的 Catalog / Namespace API。客户端不再直接提交 Lance 表，而是先把 Lance transaction 提交给 Catalog，由服务端完成提交。

```text
Engine / Lance SDK
  ↓
生成 Lance transaction
  ↓
调用 Catalog.CommitTable(transaction)
  ↓
Catalog 做权限、策略、审计和冲突处理
  ↓
Catalog 按 Lance commit protocol 写入新的 manifest
  ↓
新版本出现在 _versions 目录
```

它本质上是“提交接口”，而不是“变更通知”。它的关键特征是：

```text
Catalog 管提交入口
Lance 仍通过 _versions / manifest 规则判断最新版本
```

也就是说，`CommitTable` 并不必然等同于 Iceberg 式 latest pointer 模型。

### 9.2 如何实现

从实现角度看，`CommitTable` 的关键是：Catalog 接收的是一次“表级提交请求”，而不是简单登记某个版本号。

一个最小可用接口可以抽象为：

```text
CommitTable(
  table_id,
  branch,
  read_version,
  transaction,
  idempotency_key,
  request_context
) -> CommitTableResponse
```

其中：

```text
read_version：
  客户端读取时看到的基线版本

transaction：
  本次变更的结构化描述

idempotency_key：
  防止重试导致重复提交

request_context：
  操作者、租户、作业、来源引擎等治理上下文
```

返回结果可包含：

```text
previous_version
new_version
manifest_location
commit_status
conflict_type
retryable
audit_id
```

一个典型的服务端提交流程如下：

```text
1. 接收 CommitTable(request)
2. 校验表、branch、权限、配额、策略
3. 读取当前最新版本
4. 检查 read_version 是否过期
5. 如有版本差异，则执行冲突检测
6. 如可自动合并，则 rebase transaction
7. 由服务端生成新 manifest
8. 按 Lance commit protocol 尝试提交新版本
9. 记录审计并触发下游事件
10. 返回 new_version 与 commit_status
```

方案四的重点在于：服务端是否真正接管了提交执行权。

### 9.3 冲突处理与权限控制

方案四比方案五更重，一个重要原因是它要求 Catalog 更深地介入冲突处理。

例如：

```text
writer A 基于 v10 生成 transaction_A
writer B 也基于 v10 生成 transaction_B
A 先提交成功，表变成 v11
B 再调用 CommitTable 时，read_version 已过期
```

此时 Catalog 服务端不能简单失败或盲目覆盖，而是需要判断：

```text
transaction_B 是否应直接失败
transaction_B 是否可自动 rebase 到 v11
transaction_B 是否可重试生成 v12
```

因此，Catalog 服务端必须复用或实现与 Lance 一致的冲突检测、rebase 和重试语义；否则，同一种 transaction 在“客户端直提”和“Catalog 代提”两条路径上会表现不一致。

权限控制同样关键。典型做法是：

```text
普通 writer 可以写 data / index / deletion files
普通 writer 不能直接写 _versions
只有 Catalog commit service 可以写 _versions
```

否则用户仍然可以绕过 `CommitTable` 直接提交。

### 9.4 优点、边界与当前状态

优点：

```text
Catalog 可以前置权限控制
Catalog 可以完整审计
Catalog 可以触发下游流程
Lance 仍保留 manifest 命名规则
比 Implementation-managed 对 Lance 侵入更低
```

局限性：

```text
Catalog 需要更深地理解 Lance transaction
服务端必须承接 Lance 原生冲突检测、rebase 和重试语义
实现复杂度高于纯版本协调接口
如果实现方式退化为 pointer-based latest version 模型，则会破坏 Lance 原生版本语义
```

与方案五相比，方案四是表级提交接口，方案五是底层版本协调接口。二者并不冲突，常见做法是：

```text
对外暴露 CommitTable
对内复用 ManifestStore / CommitHandler
```

这里还需要区分两层含义：

```text
#5229 中的 CommitTable：
  更偏 transaction-level commit。
  即 Catalog 接收 Lance transaction，
  并由 Catalog / Namespace 服务端执行完整 Lance commit protocol。

当前公开 Namespace 规范中的相关模型：
  更偏 metadata-level commit。
  例如 BatchCommitTables / CommitTableOperation、
  以及 CreateTableVersion / BatchCreateTableVersions 这类接口，
  主要围绕表元数据与 TableVersion 的批量原子提交展开。
```

因此，不能简单认为 Lance 已经完整落地了 #5229 中设想的“接收任意 Lance transaction 并完成完整表提交”的 CommitTable API。

从公开信息看，`CommitTable` 仍更像设计提案，而不是已经完整落地的稳定 API。需要特别区分的是，#5849 明确保留意见的，主要是**更接近 Iceberg 的 pointer-based commitTable 实现方式**，即由 Catalog pointer 决定最新版本，而不是继续沿用 Lance 递增版本号与 `_versions` 语义；这并不等同于否定所有“Catalog 参与 commit”的接口设计。

因此，若采用方案四，更稳妥的实现方式是：将 `CommitTable` 作为对外接口，而底层继续保留 Lance 的增量版本语义与 ManifestStore / CommitHandler 机制。

------

## 10. 方案五：ManifestStore / Catalog Intercept Commit

### 10.1 基本思路

`ManifestStore / Catalog Intercept Commit` 不是 #5229 中直接列出的正式方案名称，而是基于后续公开讨论整理出的工程抽象。它描述的是一种**比 CommitTable 更底层的版本协调方向**：Catalog 不一定直接接管完整 transaction，而是在 manifest 版本创建阶段介入。

```text
Engine / Lance Runtime
  ↓
生成数据文件和 manifest
  ↓
调用 Catalog / ManifestStore 创建新版本
  ↓
Catalog 做权限、版本登记、审计和冲突协调
  ↓
新版本生效
```

它的核心不是“提交一次表 transaction”，而是“谁有权让某个版本正式生效”。

### 10.2 如何实现

ManifestStore 可理解为管理 `version -> manifest_location` 映射的版本协调层。一个最小接口可以抽象为：

```text
GetLatestVersion(table_id) -> (version, manifest_location)
GetVersion(table_id, version) -> manifest_location
CreateVersionIfNotExists(table_id, version, manifest_location, size, etag)
DeleteTableVersions(table_id)   // 可选
```

从当前公开讨论看，Lance 已存在 `ExternalManifestStore` 抽象，其核心能力包括：

```text
get(base_uri, version)
get_latest_version(base_uri)
put_if_not_exists(base_uri, version, path, size, e_tag)
put_if_exists(base_uri, version, path, size, e_tag)
delete(base_uri)
```

这说明“外部版本协调层”并非纯理论概念，而是 Lance 当前提交路径中已经存在的能力雏形。

从当前公开的 Namespace 规范方向看，已经比较接近这一思路的接口模型是 TableVersion 级别的版本管理能力，例如：

```text
CreateTableVersion
ListTableVersions
DescribeTableVersion
BatchCreateTableVersions
```

这类接口的核心不是接收完整 Lance transaction，而是参与：

```text
version -> manifest_location
```

这一关键版本登记环节。因此，更准确地说，当前社区公开规范中更清晰的落地形态，不是 Iceberg 式 latest pointer，也不完全等同于 #5229 中 transaction-level 的 CommitTable 设想，而是更偏向：

```text
Catalog / Namespace 参与 TableVersion 管理
Catalog / Namespace 作为版本协调层
Lance 继续保留递增版本号和 manifest 命名语义
```

一个典型的单表提交流程如下：

```text
1. Writer 读取当前最新版本
2. Writer 基于该版本生成 transaction
3. Writer 写入 data / index / deletion files
4. Writer 生成新 manifest，并先写入对象存储
5. CommitHandler 调用 ManifestStore：
     CreateVersionIfNotExists(table_id, next_version, manifest_location)
6. 创建成功，则 next_version 生效
7. 创建失败，则说明已有其他 writer 先提交成功，进入冲突处理 / 重试
```

### 10.3 冲突处理与权限控制

方案五不会消除 Lance 原有并发冲突，而是把“哪个版本号已被占用”这一事实统一交给 ManifestStore 判断。

例如：

```text
writer A 和 writer B 都读取到 v10
二者都生成基于 v10 的新 manifest
二者都尝试创建 v11
```

此时 ManifestStore 只允许一个 writer 成功创建 `v11`；另一个 writer 需要重新读取最新版本、重新做冲突检测，必要时 rebase transaction，再尝试创建 `v12`。

因此，职责可以拆分为：

```text
ManifestStore：
  判断版本号是否已被占用

Lance Runtime / CommitHandler：
  根据最新版本执行冲突检测、rebase 和重试
```

权限模型通常需要收紧到：

```text
普通 writer：
  允许写 data / index / deletion files
  不允许直接写 _versions

ManifestStore / Catalog commit service：
  允许登记 version -> manifest_location
  必要时允许最终确认 _versions 元数据
```

这里需要注意，ManifestStore 不一定直接保存 manifest 内容本身，也不一定总是直接写 `_versions` 文件。更通用的理解是：

```text
manifest 内容：
  仍然存放在对象存储或文件系统中

ManifestStore：
  负责原子登记某个 version 对应哪个 manifest_location
  并协调并发提交时哪个 writer 能让新版本正式生效
```

### 10.4 与方案四的区别

方案四与方案五都让 Catalog 参与 commit，但介入深度不同：

```text
方案四：
  Catalog 接收 transaction，并执行完整提交

方案五：
  Catalog 不一定理解完整 transaction，
  主要负责 version -> manifest_location 的原子登记
```

可以把二者理解为：

```text
CommitTable：
  表级提交接口

ManifestStore：
  底层版本协调接口
```

可以用一个最小例子理解二者差异。假设表 `image_embeddings` 当前版本为 `v10`，有一个 writer 要 append 一批新数据。

在方案四中，writer 会：

```text
1. 基于 v10 生成 transaction
2. 调用 Catalog.CommitTable(transaction)
3. 由 Catalog 服务端检查权限、处理冲突、生成 manifest 并提交 v11
```

也就是说，方案四里 Catalog 接管的是：

```text
一次完整的表提交
```

在方案五中，writer 会：

```text
1. 基于 v10 生成 transaction
2. 自己生成新 manifest
3. 调用 ManifestStore：
   CreateVersionIfNotExists(table_id, version=11, manifest_location=...)
4. 由 ManifestStore 判断 v11 能否正式生效
```

也就是说，方案五里 Catalog 接管的是：

```text
一个新版本是否可以创建成功
```

因此，可以用一句话概括：

```text
方案四：
  Catalog 接管一次完整表提交

方案五：
  Catalog 协调一个新版本是否正式生效
```

从当前公开讨论的演进看，Lance 社区更倾向于保留递增版本号、`_versions` 和原生冲突解决语义，并将 Catalog 介入点下沉到 ManifestStore / CommitHandler 这一层。

此外，ManifestStore / TableVersion 方向的价值不只在单表 commit，也体现在多表事务和分区表场景中。例如在 partitioned namespace 场景下，一次业务写入可能需要同时更新多张 partition table 的版本。如果只逐表提交，就可能出现部分成功、部分失败的问题。因此，像 `BatchCreateTableVersions`、`BatchCommitTables` 这类能力的意义在于：

```text
让多个版本登记操作具备原子性
要么全部成功
要么全部失败
```

这对分区表原子更新、多张相关表同时提交、跨表 schema evolution 协调，以及训练数据与索引版本同步等场景都很重要。

------

## 11. 方案对比

### 11.1 总体对比

| 方案 | 提交流程 | 最新版本来源 | Catalog 角色 | 治理能力 | 对 Lance 原生语义侵入性 | 实现复杂度 | 主要问题 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Storage-managed | Engine / SDK 直接提交 Lance 表 | `_versions` manifest 规则 | 可选登记，不在提交链路 | 弱 | 低 | 低 | Catalog 不一定感知 commit |
| Storage-managed + Notification | Engine / SDK 先提交 Lance，再通知 Catalog | `_versions` manifest 规则 | 事后观察者 | 中 | 低 | 中低 | 可能漏通知、通知失败、无法前置控制 |
| Implementation-managed | Engine 通过 Catalog 提交 | Catalog latest pointer | 提交控制者 + 最新版本认定者 | 强 | 高 | 高 | 削弱 Lance 自描述能力，branching 复杂 |
| CommitTable | Engine 把 transaction 交给 Catalog，由 Catalog 提交 Lance | `_versions` manifest 规则 | 提交入口和执行者 | 强 | 中 | 高 | Catalog 需要理解 transaction 和 commit protocol |
| ManifestStore / Intercept | Engine / Runtime 生成 manifest，Catalog 参与版本登记 | ManifestStore + Lance 版本语义 | 版本协调者 | 强 | 低到中 | 中高 | 需要定义一致的版本协调接口 |

### 11.2 核心差异

谁执行提交：

```text
Storage-managed / Notification：
  Engine / Lance SDK

Implementation-managed / CommitTable：
  Catalog / Namespace

ManifestStore / Intercept：
  Lance Runtime 与 Catalog ManifestStore 协同
```

谁决定最新版本：

```text
Storage-managed / Notification / CommitTable：
  Lance _versions / manifest 命名规则

Implementation-managed：
  Catalog latest pointer

ManifestStore / Intercept：
  ManifestStore 版本登记 + Lance 版本语义
```

治理能力与侵入性对比结论：

```text
治理能力最弱：
  Storage-managed

可作为过渡补充：
  Storage-managed + Notification

控制范围最大、治理强但侵入性大：
  Implementation-managed

治理强且较好保留 Lance 版本语义：
  CommitTable / ManifestStore
```

------

## 12. 最终方案推荐

基于当前公开讨论与工程可行性分析，本文建议将 **ManifestStore / Catalog Intercept Commit** 作为最终推荐方向，将 **CommitTable** 视为可接受但更适合作为上层接口的方案。

### 12.1 主推荐：ManifestStore / Catalog Intercept Commit

推荐理由：

```text
保留 Lance 增量版本号和 manifest 命名语义
保留 Lance 原生 time-travel / rollback / version comparison 能力
避免引入 Iceberg 式 latest pointer
更适合 branching 场景
便于 Catalog 介入版本登记、冲突协调、审计和权限控制
与 #5849 中 ManifestStore 的公开演进方向更一致
也与当前 Namespace TableVersion 接口方向更接近
```

其核心是：

```text
Lance Runtime 继续负责 transaction 语义、manifest 生成、冲突检测和 rebase
Catalog / ManifestStore 负责 version -> manifest_location 的原子登记
谁能让新版本生效，由 ManifestStore / TableVersion 机制统一协调
```

这种方案的关键不是让 Catalog 成为 latest pointer 的唯一 source of truth，而是让 Catalog 成为版本创建和治理控制点。

### 12.2 次优方案：CommitTable

如果需要为上层引擎提供统一提交入口，`CommitTable` 仍然是合理方案，但更适合作为：

```text
架构接口层
Catalog-facing submit API
或 ManifestStore 之上的上层封装
```

其约束在于：

```text
公开规范尚未稳定
Catalog 需要更深地理解 Lance transaction
实现复杂度高于纯版本协调接口
若退化为 pointer-based latest version 模型，会破坏 Lance 原生版本语义
```

因此，更稳妥的做法是：

```text
对外提供 CommitTable
对内复用 ManifestStore / CommitHandler
继续保留 Lance 递增版本号与 _versions 语义
```

### 12.3 不推荐作为最终方向：Implementation-managed table

虽然 `Implementation-managed` 治理能力强，但不建议作为 Lance 的长期方向，主要原因是：

```text
需要引入 Catalog latest pointer
削弱 Lance 自描述版本结构
使 branching 管理复杂化
可能形成 Catalog truth 与 storage truth 分离
```

### 12.4 Storage-managed + Notification 的定位

`Storage-managed + Notification` 适合作为低成本补充治理机制，但不应作为最终提交架构。它适合补审计、补血缘、补事件通知，不适合承担最终一致性与前置控制职责。

### 12.5 采用 ManifestStore / Catalog Intercept 的前提与风险

虽然 ManifestStore / Catalog Intercept 更适合作为长期方向，但落地时仍需明确几个前提。

第一，需要明确 storage 与 ManifestStore 的 source-of-truth 边界：

```text
Storage：
  保存 manifest 文件和物理数据

ManifestStore / Catalog：
  负责版本登记、并发协调、权限控制和审计
```

二者之间需要有一致性恢复机制，避免出现 manifest 已写入对象存储但版本登记失败，或者版本已登记但 manifest 不可读的情况。

第二，需要明确并发冲突后的处理语义：

```text
版本号已存在时如何处理
哪些 transaction 可以 rebase
哪些冲突必须失败
重试是否幂等
失败后是否需要清理临时 manifest
```

第三，需要配合对象存储权限控制。如果普通 writer 仍可直接写 `_versions`，就仍然可以绕过 Catalog / ManifestStore 直接提交。

第四，需要考虑 Catalog / ManifestStore 不可用时的读取语义。理想情况下，Lance 表仍应尽量保留 storage-only 可读能力，避免完全丧失开放表格式的可移植性。

------

## 13. 结论

Lance discussion #5229 的核心问题是：

> 如何在保留 Lance 自身版本管理机制的同时，让 Catalog 感知并控制表提交。

本文的结论如下：

```text
最终推荐：
  ManifestStore / Catalog Intercept Commit

可接受但更适合作为上层接口：
  CommitTable

不建议作为 Lance 最终方向：
  Implementation-managed table

仅适合作为过渡或补充治理：
  Storage-managed + Catalog Notification
```

总体来看，公开讨论并不是在否定“Catalog 参与 commit”本身，而是在避免将 Lance 改造成类似 Iceberg 的 pointer-based latest version 模型。

相较之下，更合理的方向是：

```text
保留 Lance 递增版本号、_versions 和 manifest 命名规则
让 Catalog / Namespace 参与提交入口或版本登记
通过 ManifestStore / TableVersion 机制完成并发协调与治理控制
必要时在其上封装 CommitTable / CommitAsset 作为上层 API
```

因此，Lance 更适合采用“Catalog 参与版本协调，而非 Catalog 接管 latest pointer”的路线。这一路线既能增强 Catalog 的治理能力，又能保留 Lance 的开放表格式、自描述版本、time travel、rollback、version comparison、branching 和冲突处理语义。
