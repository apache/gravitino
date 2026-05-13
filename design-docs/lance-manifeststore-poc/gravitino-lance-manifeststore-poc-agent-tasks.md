# Gravitino Lance ManifestStore POC Agent 开发任务书

## 1. 任务目标

本文给后续开发 agent 使用，目标是在 Apache Gravitino 基础上完整开发、测试并验证 Lance 方案五 POC：

```text
Gravitino Lance REST service 作为 Lance table version registration coordinator。
```

核心验收：

1. 支持 `CreateTableVersion`。
2. 支持 `ListTableVersions`。
3. 支持 `DescribeTableVersion`。
4. 支持 `BatchCreateTableVersions`。
5. 支持 `BatchDeleteTableVersions`。
6. 单表同一 version 并发创建时只有一个成功。
7. Batch create 具备全有或全无语义。
8. 结论报告明确说明：本 POC 是 version registration，不是 transaction-level commit。

## 2. 输入文档

开发前必须阅读：

1. `design-docs/lance-manifeststore-poc/lance-catalog-aware-table-commits-research.md`
2. `design-docs/lance-manifeststore-poc/gravitino-lance-manifeststore-poc-requirements.md`
3. `design-docs/lance-manifeststore-poc/gravitino-lance-manifeststore-poc-design.md`

开发目标以本文为执行拆解，以上两份 POC 文档为需求和设计依据。

## 3. 工作原则

### 3.1 边界原则

必须保持以下边界：

```text
不实现 CommitTable(transaction)
不解析 Lance transaction
不生成 Lance manifest
不做 transaction rebase
不维护 catalog latest pointer
不改 Lance 原生 _versions 版本语义
```

POC 只实现：

```text
table_id + version -> manifest_path
```

的原子登记、查询、删除和 batch 登记。

### 3.2 开发原则

1. 优先复用 Gravitino 现有 REST resource、operation、metadata store、error mapping、transaction 管理模式。
2. 不为了 POC 引入独立服务框架。
3. 不绕过 Gravitino 已有鉴权、命名空间解析、catalog/schema/table 元数据模型。
4. 对 batch 原子性必须写测试证明。
5. 如果发现 Gravitino 当前版本已有类似能力，应优先复用或补齐，而不是重复造一套。

## 4. 总体开发阶段

```text
阶段 0：代码探索与结论记录
阶段 1：API model 与 route 接入
阶段 2：metadata schema 与 DAO
阶段 3：service / operation 实现
阶段 4：并发与 batch 原子性
阶段 5：单元测试与集成测试
阶段 6：端到端 POC 脚本
阶段 7：开发结论报告
```

每个阶段都必须有可提交结果，不能只写分析。

## 5. 阶段 0：代码探索任务

### 5.1 目标

定位 Gravitino 中 Lance REST service 的代码路径、metadata store 扩展方式、测试基础设施。

### 5.2 必查代码点

后续 agent 需要在 Gravitino 仓库中查找并记录：

| 编号 | 要找什么 | 预期输出 |
| --- | --- | --- |
| E0-1 | Lance REST resource 类 | 文件路径、已有 route |
| E0-2 | Lance table create/register/describe 实现 | operation/service 文件路径 |
| E0-3 | Gravitino table metadata model | table id 如何表示、rename/drop 如何处理 |
| E0-4 | metadata store DAO/mapper 模式 | 新增表如何建模 |
| E0-5 | DB migration 机制 | schema 文件路径、测试如何初始化 |
| E0-6 | transaction 管理工具 | batch create 应如何包事务 |
| E0-7 | REST error mapping | Lance error code 如何映射 HTTP |
| E0-8 | Lance REST integration test 基类 | 如何启动服务、创建 metalake/catalog/schema/table |

### 5.3 交付物

新增或更新开发笔记：

```text
docs/dev/lance-manifeststore-code-map.md
```

如果项目没有 `docs/dev`，可放到 POC 分支说明文档中。内容至少包含：

```text
关键类路径
调用链
测试入口
需要修改的文件列表
风险点
```

## 6. 阶段 1：API Model 与 Route 接入

### 6.1 新增 Route

需要实现以下 Lance REST routes：

```text
POST /lance/v1/table/{id}/version/create
GET  /lance/v1/table/{id}/version/list
POST /lance/v1/table/{id}/version/describe
POST /lance/v1/table/version/batch-create
POST /lance/v1/table/{id}/version/batch-delete
```

如果 Gravitino 当前 route 前缀不同，应遵循现有 Lance REST service 前缀。

### 6.2 Request / Response Model

新增或复用以下模型：

```text
CreateTableVersionRequest
CreateTableVersionResponse
ListTableVersionsRequest / Response
DescribeTableVersionRequest / Response
BatchCreateTableVersionsRequest / Response
BatchDeleteTableVersionsRequest / Response
TableVersion
CreateTableVersionEntry
```

字段至少包含：

```text
id
version
manifestPath
manifestSize
eTag
namingScheme
metadata
context
```

### 6.3 验收

1. route 能被 REST framework 注册。
2. 空请求、非法 JSON、缺 version、缺 manifestPath 能返回 `InvalidInput`。
3. route 测试能证明请求进入 resource 方法。

## 7. 阶段 2：Metadata Schema 与 DAO

### 7.1 新增持久化表

建议表名：

```text
lance_table_version_meta
```

逻辑字段：

```text
metalake_id
catalog_id
schema_id
table_id
version
manifest_path
manifest_size
e_tag
naming_scheme
metadata_json
created_at
created_by
request_id
```

唯一约束：

```text
(metalake_id, catalog_id, schema_id, table_id, version)
```

如果 Gravitino metadata store 使用不同 id 命名或 composite key 方式，应跟随项目风格。

### 7.2 DAO 能力

需要实现：

```text
insert(versionMeta)
insertBatch(versionMetas)
get(tableId, version)
list(tableId, descending, limit, pageToken)
deleteBatch(tableId, versions, ignoreMissing)
deleteAllByTable(tableId)
```

`deleteAllByTable` 用于 table drop 时清理版本登记，可视项目实际 drop 流程决定是否接入。

### 7.3 验收

1. DB migration 能执行。
2. DAO insert/get/list/delete 单测通过。
3. duplicate key 单测能稳定触发。
4. metadata json 可以 round-trip。

## 8. 阶段 3：Service / Operation 实现

### 8.1 新增服务类

建议新增：

```text
LanceTableVersionOperations
```

或遵循 Gravitino 当前命名：

```text
GravitinoLanceTableVersionOperations
```

### 8.2 CreateTableVersion 逻辑

伪代码：

```text
createTableVersion(request):
  tableIdent = parseAndValidate(request.id or route id)
  validate version > 0
  validate manifestPath not empty
  table = loadTable(tableIdent)
  check table exists and provider is Lance
  check permission hook
  meta = buildVersionMeta(table, request, context)
  try insert(meta)
    return response(meta)
  catch duplicate key
    throw TableVersionAlreadyExists
```

### 8.3 ListTableVersions 逻辑

```text
listTableVersions(id, descending, limit, pageToken):
  load table
  query DAO
  convert rows to TableVersion model
```

### 8.4 DescribeTableVersion 逻辑

```text
describeTableVersion(id, version):
  load table
  row = DAO.get(table, version)
  if missing -> TableVersionNotFound
  return row
```

### 8.5 BatchCreateTableVersions 逻辑

```text
batchCreate(entries):
  validate entries non-empty
  validate no duplicate table_id + version inside request
  load all tables
  check permissions
  transaction:
    insert all version rows
  duplicate -> rollback -> TableVersionAlreadyExists
```

### 8.6 BatchDeleteTableVersions 逻辑

```text
batchDelete(id, versions, ignoreMissing):
  load table
  delete version rows
  if missing and !ignoreMissing -> TableVersionNotFound
```

### 8.7 验收

1. service 层单测覆盖正常和异常路径。
2. duplicate key 映射为 Lance Namespace 语义错误。
3. service 不读写 Lance transaction。
4. service 不维护 latest pointer。

## 9. 阶段 4：并发与 Batch 原子性

### 9.1 单表并发

必须写测试：

```text
10 个线程同时 create 同一 table 的 version=3。
期望：
  success count = 1
  conflict count = 9
  DB row count for version=3 = 1
```

### 9.2 Batch 原子性

必须写测试：

```text
预置 table_a version=2。
请求 batch create:
  table_a version=2
  table_b version=2
期望：
  batch 失败
  table_a version=2 仍只有原记录
  table_b version=2 不存在
```

### 9.3 实现要求

1. Batch 必须使用 Gravitino metadata store transaction。
2. 如果底层不支持事务，必须在代码注释、测试和最终报告中声明该 POC 不满足强原子。
3. 不允许“部分成功但返回失败”的静默行为。

## 10. 阶段 5：测试清单

### 10.1 单元测试

| 编号 | 测试 | 期望 |
| --- | --- | --- |
| U1 | CreateTableVersionRequest 缺 version | InvalidInput |
| U2 | CreateTableVersionRequest 缺 manifestPath | InvalidInput |
| U3 | DAO insert/get | 返回字段一致 |
| U4 | DAO duplicate insert | duplicate key |
| U5 | metadata json round-trip | map 不丢字段 |
| U6 | Describe missing version | TableVersionNotFound |
| U7 | Batch request 内部重复 entry | InvalidInput |
| U8 | Batch duplicate existing version | rollback |

### 10.2 集成测试

| 编号 | 测试 | 期望 |
| --- | --- | --- |
| I1 | 启动 Lance REST service | health 正常 |
| I2 | 创建 metalake/catalog/schema/table | table 可 describe |
| I3 | create version v1 | 成功 |
| I4 | list versions | 包含 v1 |
| I5 | describe v1 | manifestPath 正确 |
| I6 | duplicate create v1 | 409 |
| I7 | concurrent create v2 | 只有一个成功 |
| I8 | batch create table_a/table_b | 全部成功 |
| I9 | batch conflict rollback | 无部分写 |
| I10 | batch delete | version 不再可 describe |

### 10.3 端到端脚本

需要提供一个可复跑脚本，建议：

```text
dev/lance-manifeststore-poc/run_poc.sh
```

或项目等价脚本路径。

脚本步骤：

```text
1. start gravitino server
2. start lance rest service
3. create metalake
4. create lance catalog
5. create schema
6. create/register table
7. create version
8. duplicate create version
9. batch create
10. batch conflict
11. print summary
```

如果项目使用 Gradle/Maven integration test，不强制 shell 脚本，但必须提供一条命令跑完整 POC 测试。

## 11. 阶段 6：端到端验证命令模板

后续 agent 应在最终报告中填入真实命令。模板如下：

```bash
# build
./gradlew build -x test

# unit tests
./gradlew test --tests '*LanceTableVersion*'

# integration tests
./gradlew integrationTest --tests '*LanceTableVersion*'

# optional POC script
./dev/lance-manifeststore-poc/run_poc.sh
```

如果 Gravitino 使用 Maven：

```bash
mvn -DskipTests package
mvn -Dtest='*LanceTableVersion*' test
mvn -DskipUTs -Dit.test='*LanceTableVersion*' verify
```

最终以实际项目构建系统为准，不要机械照抄。

## 12. 阶段 7：最终开发报告

后续 agent 开发完成后，必须新增：

```text
design-docs/lance-manifeststore-poc/gravitino-lance-manifeststore-poc-implementation-report.md
```

报告至少包含：

```text
1. 修改文件列表
2. 新增 API 列表
3. 新增 metadata schema
4. 并发控制实现方式
5. Batch 原子性实现方式
6. 测试命令和结果
7. 已验证语义
8. 未验证语义
9. 已知风险
10. 后续接入 Lance Runtime 的建议
```

## 13. 推荐文件修改范围

具体路径需以后续 agent 在 Gravitino 仓库中的探索为准。预计修改范围如下：

```text
lance-rest-server module:
  - REST resource
  - request/response model
  - error mapping
  - integration tests

core / api module:
  - TableVersion model if shared
  - operation interface if needed

metadata store module:
  - schema migration
  - DAO / mapper
  - transaction handling

test module:
  - unit tests
  - integration tests
  - concurrency tests
```

## 14. 关键验收断言

后续 agent 的测试中必须出现这些断言：

```text
assert create version v1 returns success
assert duplicate create version v1 returns conflict
assert list versions contains v1
assert describe version v1 returns same manifestPath
assert concurrent create same version has exactly one success
assert batch create success inserts all entries
assert batch create conflict inserts none
assert no latest pointer field is introduced
assert code path does not parse Lance transaction
```

## 15. 失败处理要求

如果开发中发现 Gravitino 当前版本无法支持某项能力，应按以下方式处理：

1. 不能静默跳过。
2. 在实现报告中写清楚阻塞点。
3. 给出替代方案和影响。
4. 如果 batch 原子性无法实现，必须将 POC 结论降级为“单表版本登记可行，batch 原子性未验证”。

## 16. 后续增强任务

本 POC 完成后，可以继续拆以下增强任务：

1. 接入 Lance Runtime `ExternalManifestStore` / CommitHandler。
2. 支持 staging manifest existence check。
3. 支持 staging manifest finalize 到 `_versions` final path。
4. 支持 failed staging cleanup。
5. 支持 idempotency key。
6. 接入 Gravitino audit/event listener。
7. 接入对象存储 credential vending，收口 `_versions` 写权限。
8. 支持 branch / tag / rollback 语义。

## 17. Definition of Done

开发任务完成必须同时满足：

1. 代码编译通过。
2. 单元测试通过。
3. 集成测试通过。
4. 并发测试通过。
5. Batch rollback 测试通过。
6. 端到端 POC 可复现。
7. 实现报告已写。
8. 报告明确说明本 POC 的边界和未覆盖项。

## 18. Agent 执行提示

后续 agent 开始开发时，建议第一条消息执行：

```text
1. 定位 Gravitino 仓库根目录。
2. 搜索 Lance REST service。
3. 输出 code map。
4. 根据本文阶段拆分 todo。
5. 先实现最小 CreateTableVersion，再扩展 list/describe/batch。
```

不要一上来改大量无关抽象。先跑通单表 version create，再逐步补完整能力。
