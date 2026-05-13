# Gravitino Lance ManifestStore POC Code Map

## 仓库位置

`D:\ai\project\gravitino`

## 当前分支

`main`

## 关键模块

### Lance REST 入口

1. `lance/lance-rest-server/src/main/java/org/apache/gravitino/lance/LanceRESTService.java`
2. `lance/lance-rest-server/src/main/java/org/apache/gravitino/lance/service/rest/LanceTableOperations.java`
3. `lance/lance-rest-server/src/main/java/org/apache/gravitino/lance/service/rest/LanceNamespaceOperations.java`

### Lance 通用操作接口

1. `lance/lance-common/src/main/java/org/apache/gravitino/lance/common/ops/LanceTableOperations.java`
2. `lance/lance-common/src/main/java/org/apache/gravitino/lance/common/ops/LanceNamespaceOperations.java`

### Gravitino Lance 实现

1. `lance/lance-common/src/main/java/org/apache/gravitino/lance/common/ops/gravitino/GravitinoLanceTableOperations.java`
2. `lance/lance-common/src/main/java/org/apache/gravitino/lance/common/ops/gravitino/GravitinoLanceNameSpaceOperations.java`

### 现有版本相关实现

1. `core/src/main/java/org/apache/gravitino/storage/relational/service/TableMetaService.java`
2. `core/src/main/java/org/apache/gravitino/storage/relational/mapper/TableVersionMapper.java`
3. `core/src/main/java/org/apache/gravitino/storage/relational/mapper/provider/base/TableVersionBaseSQLProvider.java`

说明：

当前 `TableVersionMapper` / `TableMetaService` 处理的是 Gravitino table metadata 自身的版本时间线，不是 Lance Namespace 的 `table_id + version -> manifest_path` 版本登记模型，后续大概率需要单独的 Lance table version metadata 存储。

## 已确认路由

### Table

`LanceTableOperations.java` 当前已支持：

1. `/v1/table/{id}/describe`
2. `/v1/table/{id}/create`
3. `/v1/table/{id}/create-empty`
4. `/v1/table/{id}/declare`
5. `/v1/table/{id}/register`
6. `/v1/table/{id}/deregister`
7. `/v1/table/{id}/exists`
8. `/v1/table/{id}/drop`
9. `/v1/table/{id}/drop_columns`
10. `/v1/table/{id}/alter_columns`

当前还没有 TableVersion 相关 route。

### Namespace

`LanceNamespaceOperations.java` 当前已支持：

1. `/v1/namespace/{id}/list`
2. `/v1/namespace/list`
3. `/v1/namespace/{id}/describe`
4. `/v1/namespace/{id}/create`
5. `/v1/namespace/{id}/drop`
6. `/v1/namespace/{id}/exists`
7. `/v1/namespace/{id}/table/list`

## 已确认测试入口

### REST 资源测试

1. `lance/lance-rest-server/src/test/java/org/apache/gravitino/lance/service/rest/TestGravitinoLanceTableOperations.java`
2. `lance/lance-rest-server/src/test/java/org/apache/gravitino/lance/service/rest/TestLanceNamespaceOperations.java`

### 集成测试

1. `lance/lance-rest-server/src/test/java/org/apache/gravitino/lance/integration/test/LanceRESTServiceIT.java`
2. `lance/lance-rest-server/src/test/java/org/apache/gravitino/lance/integration/test/LanceSparkRESTServiceIT.java`

## 初步实现判断

推荐的落点：

1. 在 `lance-common` 增加 TableVersion 操作接口和 Gravitino 实现。
2. 在 `lance-rest-server` 增加 TableVersion REST route。
3. 在 `core` relational metadata store 增加 Lance table version metadata table、mapper、service。
4. 在 `lance-rest-server` 增加资源测试和集成测试。

## 风险点

1. Lance Namespace 依赖的 `org.lance.namespace.model` 是否已包含 `CreateTableVersion` 等模型，需要继续确认。
2. Gravitino metadata store 是否已有可直接复用的事务模板，需要继续确认。
3. 如果 Lance REST service 的测试基类依赖外部存储或 Spark，端到端验证成本可能比纯 REST 资源测试更高。
