# Gravitino Lance ManifestStore 方案五 POC 需求文档

## 1. 背景

本文面向后续 agent 在 Apache Gravitino 基础上验证 Lance 方案五，即 `ManifestStore / Catalog Intercept Commit`。

方案五的核心不是让 Gravitino 接收完整 Lance transaction，也不是让 Gravitino 接管 latest pointer，而是让 Gravitino 在 Lance manifest 版本生效前，承担 `table_id + version -> manifest_location` 的原子登记与治理控制点。

简化目标如下：

```text
Lance Runtime / Writer:
  生成数据文件、索引文件、删除文件和候选 manifest

Gravitino Lance REST:
  原子登记 table version
  判断 version 是否已存在
  记录审计上下文
  返回版本登记结果

Lance Runtime / Writer:
  登记成功后认为版本生效
  登记失败后执行重试 / rebase / 清理 staging manifest
```

## 2. POC 目标

### 2.1 主目标

在 Gravitino Lance REST service 中验证一个最小可跑通的 TableVersion 管理闭环：

```text
CreateTableVersion
ListTableVersions
DescribeTableVersion
BatchCreateTableVersions
BatchDeleteTableVersions
```

其中 `CreateTableVersion` 必须具备 `put-if-not-exists` 语义：

```text
同一 table_id + version 只能成功登记一次。
重复登记同一 version 必须失败。
```

### 2.2 需要证明的问题

1. Gravitino 能否作为 Lance 版本登记协调点。
2. 单表并发提交同一个 version 时，是否只有一个 writer 成功。
3. 多表版本登记是否可以通过 `BatchCreateTableVersions` 表达原子意图。
4. 登记记录是否足够支持审计、追踪和后续事件触发。
5. 方案是否保留 Lance 的 `_versions` / manifest 命名语义，而不是引入 catalog latest pointer。

## 3. 非目标

本 POC 不实现以下内容：

1. 不实现 `CommitTable(transaction)`。
2. 不让 Gravitino 解析或 rebase Lance transaction。
3. 不让 Gravitino 生成 Lance manifest。
4. 不让 Gravitino 维护 Iceberg 风格的 `latest_manifest_location` pointer。
5. 不改造 Lance 原生读取路径为强依赖 Gravitino。
6. 不要求一次性覆盖 branch / tag / rollback 的完整语义。

## 4. 术语

| 术语 | 含义 |
| --- | --- |
| Lance table | 底层 Lance 表目录，包含 data、_versions、_indices 等文件 |
| staging manifest | writer 预先写入但尚未正式生效的候选 manifest |
| table version entry | Gravitino 中登记的 `table_id + version -> manifest_path` 记录 |
| managed versioning | 表版本由 namespace API 登记协调，而不是只靠直接写 `_versions` |
| final manifest path | Lance 标准 `_versions/{version}.manifest` 或 V2 naming scheme 路径 |

## 5. 用户故事

### 5.1 单 writer 提交

作为 Lance writer，我希望在写完 staging manifest 后调用 Gravitino 登记 `v11`，登记成功后该版本被认为提交成功。

验收：

```text
POST /lance/v1/table/{id}/version/create
返回 version=11
ListTableVersions 能看到 v11
DescribeTableVersion 能返回 v11 的 manifest_path、manifest_size、e_tag、metadata
```

### 5.2 并发 writer 提交同一版本

作为平台方，我希望两个 writer 同时登记同一张表的 `v11` 时，只有一个成功，另一个收到明确的版本冲突错误。

验收：

```text
writer A: create version 11 -> success
writer B: create version 11 -> TableVersionAlreadyExists 或 ConcurrentModification
最终版本列表中只有一条 v11
```

### 5.3 多表版本登记

作为上层调度系统，我希望一次业务写入涉及多张 Lance 表时，可以通过 batch API 同时登记多个 table version。

验收：

```text
POST /lance/v1/table/version/batch-create
entries = [table_a v3, table_b v8]
全部不存在 -> 全部成功
任意一个已存在 -> 整个 batch 失败，不产生部分登记
```

### 5.4 审计与治理上下文

作为治理系统，我希望每次版本登记都能保存提交人、job、trace、engine 等上下文，方便审计与下游触发。

验收：

```text
CreateTableVersion 支持 metadata / context
服务端持久化 actor、trace_id、job_id、engine、request_time
DescribeTableVersion 可返回必要元数据
日志中可追踪一次 version create 的 request id
```

## 6. API 需求

### 6.1 CreateTableVersion

请求：

```http
POST /lance/v1/table/{id}/version/create
Content-Type: application/json
```

```json
{
  "version": 11,
  "manifest_path": "s3://bucket/table/_versions/11.manifest-staging-uuid",
  "manifest_size": 4096,
  "e_tag": "abc123",
  "naming_scheme": "V2",
  "metadata": {
    "operation": "append",
    "job_id": "job-001"
  }
}
```

成功响应：

```json
{
  "version": 11,
  "manifest_path": "s3://bucket/table/_versions/11.manifest-staging-uuid",
  "manifest_size": 4096,
  "e_tag": "abc123",
  "metadata": {
    "operation": "append",
    "job_id": "job-001"
  }
}
```

错误：

| 场景 | 错误 |
| --- | --- |
| table 不存在 | `TableNotFound` |
| version 已存在 | `TableVersionAlreadyExists` |
| 并发冲突 | `ConcurrentModification` |
| manifest_path 不合法 | `InvalidInput` |
| 存储不可用 | `ServiceUnavailable` 或 `Internal` |

### 6.2 ListTableVersions

请求：

```http
GET /lance/v1/table/{id}/version/list?descending=true&limit=100
```

响应需包含：

```text
version
manifest_path
manifest_size
e_tag
timestamp_millis
metadata
```

### 6.3 DescribeTableVersion

请求：

```http
POST /lance/v1/table/{id}/version/describe
Content-Type: application/json
```

```json
{
  "version": 11
}
```

响应返回指定版本登记详情。

### 6.4 BatchCreateTableVersions

请求：

```http
POST /lance/v1/table/version/batch-create
Content-Type: application/json
```

```json
{
  "entries": [
    {
      "id": ["lance_catalog", "schema", "table_a"],
      "version": 3,
      "manifest_path": "s3://bucket/table_a/_versions/3.manifest-staging"
    },
    {
      "id": ["lance_catalog", "schema", "table_b"],
      "version": 8,
      "manifest_path": "s3://bucket/table_b/_versions/8.manifest-staging"
    }
  ]
}
```

要求：

```text
必须具备全有或全无语义。
如果底层 Gravitino 存储事务能力不足，POC 需要明确标记为限制，不能伪装成强原子。
```

### 6.5 BatchDeleteTableVersions

用于验证回滚清理和运维删除能力。POC 阶段可以只支持 metadata 删除，不强制删除底层 manifest 文件。

## 7. 数据模型需求

新增或复用一个版本登记存储模型，逻辑字段如下：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| metalake | string | 是 | Gravitino metalake |
| catalog | string | 是 | Lance catalog |
| schema | string | 是 | Lance schema |
| table | string | 是 | Lance table |
| version | long | 是 | Lance table version |
| manifest_path | string | 是 | staging 或 final manifest path |
| manifest_size | long | 否 | manifest 文件大小 |
| e_tag | string | 否 | 对象存储 eTag |
| naming_scheme | string | 否 | V1 / V2 |
| metadata | json/string map | 否 | 业务与提交元数据 |
| created_at | timestamp | 是 | 登记时间 |
| created_by | string | 否 | 操作者 |
| request_id | string | 否 | 请求 id |

唯一约束：

```text
(metalake, catalog, schema, table, version)
```

## 8. 一致性需求

### 8.1 单表一致性

`CreateTableVersion` 必须满足：

```text
如果 version 不存在，则原子创建。
如果 version 已存在，则不覆盖原记录。
```

### 8.2 Batch 一致性

`BatchCreateTableVersions` 目标语义：

```text
所有 entries 都创建成功，或者全部不创建。
```

POC 可以接受两档实现：

| 档位 | 要求 |
| --- | --- |
| P0 | 依赖 Gravitino relational store transaction 实现强原子 |
| P1 | 先实现顺序插入 + 失败补偿，但文档和测试必须明确不是强原子 |

推荐 POC 直接以 P0 为目标。

## 9. 权限与绕过控制

POC 最少需要证明：

1. Lance REST version create 路径会经过 Gravitino 服务端。
2. 可在服务端插入权限检查点。
3. 审计上下文可被记录。

POC 不要求实现对象存储 IAM 收口，但设计必须说明：

```text
如果 writer 仍能直接写 final _versions manifest，
则 Gravitino 只能提供版本登记一致性，
不能彻底阻止绕过提交。
```

## 10. 验收用例

### 10.1 基础用例

1. 启动 Gravitino server 和 Lance REST service。
2. 创建 metalake。
3. 创建 Lance catalog。
4. 创建 schema。
5. 创建或注册 Lance table。
6. 写入一个 staging manifest。
7. 调用 `CreateTableVersion` 登记 v1。
8. 调用 `ListTableVersions` 确认 v1 存在。
9. 调用 `DescribeTableVersion` 确认 metadata 正确。

### 10.2 重复登记用例

1. 对同一 table 注册 v2。
2. 再次注册 v2。
3. 第二次请求必须失败。
4. 数据库中只有一条 v2 记录。

### 10.3 并发用例

1. 启动 10 个并发请求登记同一 table 的 v3。
2. 只有 1 个请求成功。
3. 其余请求返回版本冲突或并发冲突。
4. 最终只有一条 v3 记录。

### 10.4 Batch 成功用例

1. 对 table_a v1 和 table_b v1 发起 batch create。
2. 两条记录都存在。
3. 响应包含两个 result。

### 10.5 Batch 失败用例

1. 预先创建 table_a v2。
2. 发起 batch create: table_a v2 + table_b v2。
3. batch 失败。
4. table_b v2 不应被创建。

## 11. 成功标准

POC 成功的最低标准：

1. 单表 version create/list/describe 跑通。
2. 重复 version create 能稳定失败。
3. 并发同 version create 只有一个成功。
4. Batch create 具备明确的一致性语义。
5. 文档明确说明 Gravitino 只做 version registration，不接管 Lance transaction。

POC 成功的增强标准：

1. 版本登记记录包含审计上下文。
2. 支持 request id / idempotency key。
3. 支持事件 hook 或日志，后续可接入血缘、缓存失效、训练任务触发。
4. 与 Lance Namespace SDK 的 TableVersion model 对齐。

## 12. 参考资料

1. Lance Catalog-Aware Table Commits 调研报告：`lance-catalog-aware-table-commits-research.md`
2. Lance REST Namespace Implementation Spec：<https://lance.org/format/namespace/rest/impl-spec/>
3. Lance Directory Namespace Implementation Spec：<https://lance.org/format/namespace/dir/impl-spec/>
4. Lance CreateTableVersionRequest：<https://lance.org/format/namespace/client/operations/models/CreateTableVersionRequest/>
5. Apache Gravitino Lance REST service：<https://gravitino.apache.org/docs/1.1.0/lance-rest-service/>
