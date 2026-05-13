# Gravitino Lance ManifestStore 方案五 POC 设计文档

## 1. 设计目标

本文设计一个基于 Apache Gravitino Lance REST service 的方案五 POC。目标是在 Gravitino 中增加 Lance TableVersion 登记能力，使 Gravitino 能作为 Lance 版本提交链路上的版本协调点。

核心原则：

```text
Gravitino 只登记版本，不接管 Lance transaction。
Gravitino 只判断 table_id + version 是否可以创建，不判断 transaction 是否可 rebase。
Gravitino 不维护 latest pointer，不替代 Lance _versions 语义。
```

## 2. 总体架构

```text
Lance Writer / Test Client
  |
  | 1. write data files / staging manifest
  v
Gravitino Lance REST Resource
  |
  | 2. CreateTableVersion / BatchCreateTableVersions
  v
LanceTableVersionOperations
  |
  | 3. validate table, auth, conflict, transaction
  v
Gravitino Metadata Store
  |
  | 4. insert table version row with unique constraint
  v
Version Registry Table
```

成功路径：

```text
writer 生成 manifest
writer 调用 CreateTableVersion
Gravitino 原子插入 version row
插入成功 -> version committed
插入失败 -> writer 走 Lance 侧重试 / rebase
```

## 3. 模块边界

### 3.1 REST Resource 层

负责 Lance REST 协议入口。

建议新增或扩展资源类：

```text
LanceTableVersionOperationsResource
```

或在现有 Lance table resource 中增加 version 子路由。

职责：

1. 解析 `{id}`。
2. 解析 JSON request body。
3. 解析 context headers。
4. 调用 service / operation 层。
5. 将内部异常映射为 Lance Namespace error code。

### 3.2 Operation / Service 层

建议新增：

```text
LanceTableVersionOperations
```

职责：

1. 校验 table 是否存在。
2. 校验请求字段。
3. 执行权限检查 hook。
4. 调用 metadata store 执行原子插入。
5. 返回 Lance Namespace model。

### 3.3 Metadata Store 层

建议新增 table version DAO / mapper：

```text
TableVersionMeta
TableVersionMetaMapper
TableVersionMetaService
```

如果 Gravitino 已有通用 metadata entity 扩展点，可优先复用；如果没有，POC 可以新增一张独立关系表。

## 4. API 设计

### 4.1 CreateTableVersion

路由：

```http
POST /lance/v1/table/{id}/version/create
```

请求模型：

```java
class CreateTableVersionRequest {
  List<String> id;
  Long version;
  String manifestPath;
  Long manifestSize;
  String eTag;
  String namingScheme;
  Map<String, String> metadata;
  Map<String, String> context;
}
```

响应模型：

```java
class CreateTableVersionResponse {
  TableVersion version;
}
```

核心逻辑：

```text
1. parse table id
2. validate version > 0
3. validate manifest_path is non-empty
4. load table metadata from Gravitino
5. auth check: canAlterTable / canWriteTableVersion
6. insert version row
7. if duplicate key -> TableVersionAlreadyExists
8. return created version
```

### 4.2 ListTableVersions

路由：

```http
GET /lance/v1/table/{id}/version/list
```

查询参数：

```text
descending: boolean
limit: int
page_token: string
```

POC 可先不实现复杂 pagination，但需要限制返回数量，避免无界扫描。

### 4.3 DescribeTableVersion

路由：

```http
POST /lance/v1/table/{id}/version/describe
```

核心逻辑：

```text
根据 table_id + version 查询单条 version row。
不存在时返回 TableVersionNotFound。
```

### 4.4 BatchCreateTableVersions

路由：

```http
POST /lance/v1/table/version/batch-create
```

核心逻辑：

```text
1. validate all entries
2. validate all tables exist
3. in one DB transaction:
     insert all version rows
4. if any duplicate:
     rollback transaction
     return TableVersionAlreadyExists
5. return all created versions
```

实现时必须避免：

```text
先插入一部分，遇到冲突后直接返回失败但不回滚。
```

### 4.5 BatchDeleteTableVersions

路由：

```http
POST /lance/v1/table/{id}/version/batch-delete
```

POC 只删除 metadata row，不删除对象存储 manifest 文件。

## 5. 数据模型设计

### 5.1 关系表

建议新增表：

```sql
CREATE TABLE lance_table_version_meta (
  metalake_id        BIGINT       NOT NULL,
  catalog_id         BIGINT       NOT NULL,
  schema_id          BIGINT       NOT NULL,
  table_id           BIGINT       NOT NULL,
  version            BIGINT       NOT NULL,
  manifest_path      VARCHAR(...) NOT NULL,
  manifest_size      BIGINT       NULL,
  e_tag              VARCHAR(...) NULL,
  naming_scheme      VARCHAR(...) NULL,
  metadata_json      TEXT         NULL,
  created_at         BIGINT       NOT NULL,
  created_by         VARCHAR(...) NULL,
  request_id         VARCHAR(...) NULL,
  PRIMARY KEY (metalake_id, catalog_id, schema_id, table_id, version)
);
```

说明：

1. POC 优先使用内部 numeric id，而不是名称字符串，避免 rename 后 version 记录失联。
2. `metadata_json` 保存 Lance Namespace metadata map 和审计扩展字段。
3. `created_at` 使用 epoch millis，与 Gravitino 既有时间字段风格对齐。

### 5.2 TableVersion 对象

服务层对象：

```java
class LanceTableVersionMeta {
  long metalakeId;
  long catalogId;
  long schemaId;
  long tableId;
  long version;
  String manifestPath;
  Long manifestSize;
  String eTag;
  String namingScheme;
  Map<String, String> metadata;
  long createdAt;
  String createdBy;
  String requestId;
}
```

REST 响应对象应尽量对齐 Lance Namespace `TableVersion` model。

## 6. 并发控制设计

### 6.1 单版本冲突

依赖数据库唯一约束：

```text
PRIMARY KEY (metalake_id, catalog_id, schema_id, table_id, version)
```

并发插入时：

```text
第一个事务提交成功。
后续事务触发 duplicate key。
服务层捕获并映射为 TableVersionAlreadyExists。
```

### 6.2 Batch 原子性

`BatchCreateTableVersions` 必须包裹在一个 metadata store transaction 中。

伪代码：

```java
transactionTemplate.execute(() -> {
  for (Entry entry : entries) {
    tableVersionDao.insert(entry);
  }
});
```

如果任意插入失败：

```text
rollback transaction
return error
```

### 6.3 幂等性

POC 可选支持 `idempotency_key`：

```text
metadata.idempotency_key
或 x-lance-ctx-idempotency-key
```

最低要求：

1. 同一 version 重复请求返回 conflict。
2. 不要求自动识别同一请求重放并返回 success。

增强要求：

```text
如果 duplicate row 的 request_id / idempotency_key 与当前请求一致，
可返回已有 version，视为幂等成功。
```

## 7. Manifest 文件处理

POC 采用最小策略：

```text
Gravitino 不复制 staging manifest 到 final manifest path。
Gravitino 只登记调用方传入的 manifest_path。
```

原因：

1. 能最小验证版本登记协调点。
2. 避免引入对象存储 credential / copy / cleanup 的复杂度。
3. 与后续 Lance Runtime 接入时的职责边界更清晰。

后续增强可以支持：

```text
1. validate manifest exists
2. copy staging manifest to final _versions path
3. update manifest_path from staging to final
4. cleanup failed staging manifest
```

但这些不作为 POC 必需项。

## 8. 权限与审计设计

### 8.1 权限检查点

在 `CreateTableVersion` 和 `BatchCreateTableVersions` 中预留：

```text
checkCanCreateTableVersion(actor, table)
```

POC 可先复用已有 table alter/write 权限。

### 8.2 审计字段

从 request context 提取：

```text
actor
request_id
trace_id
job_id
engine
source
```

保存策略：

```text
created_by -> actor
request_id -> request_id
metadata_json -> trace_id / job_id / engine / source
```

日志要求：

```text
每次 create table version 记录 table id、version、request id、result。
```

## 9. 错误映射

| 内部错误 | Lance Namespace 错误 | HTTP |
| --- | --- | --- |
| table not found | TableNotFound | 404 |
| version not found | TableVersionNotFound | 404 |
| duplicate version | TableVersionAlreadyExists | 409 |
| duplicate key during concurrent insert | TableVersionAlreadyExists 或 ConcurrentModification | 409 |
| invalid manifest path | InvalidInput | 400 |
| permission denied | PermissionDenied / Forbidden | 403 |
| metadata store unavailable | ServiceUnavailable | 503 |
| unknown error | Internal | 500 |

建议 POC 将 duplicate key 统一映射为 `TableVersionAlreadyExists`，更贴合 `put-if-not-exists` 语义。

## 10. 开发任务拆解

### 10.1 代码定位任务

后续 agent 先完成以下探索：

1. 找到 Gravitino Lance REST service 的 Resource 类。
2. 找到 Lance table create/register/describe 的 operation 类。
3. 找到 metadata store 的 table meta DAO / mapper / transaction 工具。
4. 找到 error code 到 HTTP response 的映射层。
5. 找到 integration test 启动 Lance REST service 的基类。

### 10.2 实现任务

建议按以下顺序开发：

1. 新增 REST route 和 request / response model。
2. 新增 `LanceTableVersionOperations` service。
3. 新增 `lance_table_version_meta` schema migration。
4. 新增 DAO / mapper。
5. 实现 `CreateTableVersion`。
6. 实现 `ListTableVersions` 和 `DescribeTableVersion`。
7. 实现 `BatchCreateTableVersions`。
8. 实现 `BatchDeleteTableVersions`。
9. 加入权限检查 hook。
10. 加入审计日志。

### 10.3 测试任务

单元测试：

1. request validation。
2. duplicate version error mapping。
3. metadata json round trip。
4. table id resolution。

集成测试：

1. create version success。
2. create duplicate version fails。
3. describe version success。
4. list versions ordered。
5. concurrent create only one succeeds。
6. batch create all success。
7. batch create conflict rollback。
8. batch delete versions。

## 11. 测试客户端设计

POC 可以先用 curl 或 Java/Python test client。

创建版本：

```bash
curl -X POST \
  "http://localhost:9101/lance/v1/table/lance_catalog%24schema%24table01/version/create" \
  -H "Content-Type: application/json" \
  -H "x-lance-ctx-request_id: req-001" \
  -d '{
    "version": 1,
    "manifest_path": "/tmp/lance_catalog/schema/table01/_versions/1.manifest-staging",
    "manifest_size": 1024,
    "e_tag": "etag-1",
    "naming_scheme": "V1",
    "metadata": {
      "operation": "append",
      "job_id": "job-001"
    }
  }'
```

列版本：

```bash
curl \
  "http://localhost:9101/lance/v1/table/lance_catalog%24schema%24table01/version/list?descending=true&limit=100"
```

描述版本：

```bash
curl -X POST \
  "http://localhost:9101/lance/v1/table/lance_catalog%24schema%24table01/version/describe" \
  -H "Content-Type: application/json" \
  -d '{"version": 1}'
```

## 12. 与 Lance Runtime 的集成假设

POC 阶段可以不改 Lance Runtime，只通过测试客户端模拟：

```text
writer 已生成 staging manifest
writer 调用 Gravitino version create
writer 根据返回结果决定成功 / 重试
```

后续真正集成时，需要在 Lance CommitHandler / ManifestStore 适配层中把：

```text
put_if_not_exists(base_uri, version, path, size, e_tag)
```

映射为：

```text
POST /lance/v1/table/{id}/version/create
```

## 13. 风险与限制

### 13.1 绕过风险

如果 writer 仍有权限直接写 final `_versions` manifest，则 Gravitino 无法阻止绕过登记。

POC 结论中必须明确：

```text
方案五需要配合对象存储权限收口，才能成为强治理提交入口。
```

### 13.2 source of truth 风险

POC 中 version registry 与 `_versions` 目录可能不一致，例如：

```text
version row 已登记，但 manifest 文件不存在。
manifest 文件存在，但 version row 不存在。
```

POC 暂不解决自动修复，但需要提供诊断能力：

```text
DescribeTableVersion 能返回 manifest_path。
测试中可以显式检查文件存在性。
```

### 13.3 Batch 原子性风险

如果 Gravitino 当前 metadata store 层不便暴露事务，batch 可能退化为非原子。

设计要求：

```text
不能把非原子实现宣传为原子实现。
测试必须覆盖部分失败场景。
```

## 14. 交付物

后续 agent 开发完成后，应交付：

1. REST API 实现。
2. metadata schema migration。
3. DAO / service / resource 层代码。
4. 单元测试。
5. 集成测试。
6. curl 或脚本化 POC 运行说明。
7. 一份 POC 结论，说明哪些方案五语义已验证，哪些仍未验证。

## 15. 验证结论模板

开发完成后建议按以下模板输出：

```text
已验证:
  - 单表 version put-if-not-exists
  - 并发同 version 只有一个成功
  - version list / describe
  - batch create 原子语义

未验证:
  - Lance Runtime 原生 CommitHandler 接入
  - 对象存储权限收口
  - staging manifest finalize / cleanup
  - branch / tag / rollback

结论:
  Gravitino 可以作为 Lance 方案五的 version registration coordinator。
  但要成为生产级 catalog-aware commit path，还需要接入 Lance Runtime 与存储权限治理。
```

## 16. 参考资料

1. 需求文档：`gravitino-lance-manifeststore-poc-requirements.md`
2. Lance Catalog-Aware Table Commits 调研报告：`lance-catalog-aware-table-commits-research.md`
3. Lance REST Namespace Implementation Spec：<https://lance.org/format/namespace/rest/impl-spec/>
4. Lance Directory Namespace Implementation Spec：<https://lance.org/format/namespace/dir/impl-spec/>
5. Apache Gravitino Lance REST service：<https://gravitino.apache.org/docs/1.1.0/lance-rest-service/>
