<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# 设计文档：Apache Gravitino 支持 AWS Glue Data Catalog

## 1. 问题陈述与目标

### 1.1 问题

**Gravitino 当前无法联邦管理 AWS Glue Data Catalog。** 这是一个显著的功能缺失：

1. **AWS 上庞大的用户群**：大部分云原生数据湖运行在 AWS 上，Glue Data Catalog 是其核心元数据服务（Athena、Redshift Spectrum、EMR、Lake Formation 的默认 metastore）。这些组织无法将 Glue 元数据纳入 Gravitino 的统一管理层。
2. **缺乏原生集成路径**：目前唯一的变通方案是将 Gravitino 的 Hive catalog 指向 Glue 的 HMS 兼容 Thrift 端点（`metastore.uris = thrift://...`），但该端点未公开文档、受地域限制，且无法使用 Glue 原生特性（catalog ID、跨账号访问、VPC 端点）。
3. **竞品对比**：Trino、Spark 等引擎均已提供一流的 Glue 支持和专用配置，用户对 Gravitino 有同样的期望。

### 1.2 目标

功能实现后：

1. **在 Gravitino 中注册 AWS Glue Data Catalog**：
   ```bash
   # Hive 格式表
   gcli catalog create --name hive_on_glue --provider hive \
     --properties metastore-type=glue,s3-region=us-east-1

   # Iceberg 格式表
   gcli catalog create --name iceberg_on_glue --provider lakehouse-iceberg \
     --properties catalog-backend=glue,warehouse=s3://bucket/iceberg,s3-region=us-east-1
   ```

2. **标准 Gravitino API 可操作 Glue catalog**：
   ```bash
   gcli schema list --catalog hive_on_glue
   gcli table list --catalog hive_on_glue --schema my_database
   gcli table details --catalog iceberg_on_glue --schema analytics --table events
   ```

3. **Trino 和 Spark 透明接入** — Trino 使用 `hive.metastore=glue` / `iceberg.catalog.type=glue`；Spark 使用 `AWSGlueDataCatalogHiveClientFactory` / `GlueCatalog`。用户通过 Gravitino 查询 Glue 表，无需了解底层机制。

4. **AWS 原生认证**（复用现有 S3 属性）：静态凭证、STS AssumeRole、或默认凭证链（环境变量、实例配置文件）。

## 2. 背景

### 2.1 AWS Glue Data Catalog

AWS Glue Data Catalog 是托管的元数据存储库，存储：
- **Database** — 逻辑分组，等同于 Gravitino 的 schema。
- **Table** — 元数据记录，包含列定义、存储描述符、分区键和用户自定义参数。

表有两种格式：

| 格式 | Glue 存储方式 |
|---|---|
| **Hive** | 完整元数据存在 `StorageDescriptor` 中（列、SerDe、InputFormat、OutputFormat、数据位置）。大多数 Glue catalog 中的主要表类型（传统 ETL、Athena CTAS、Redshift Spectrum）。 |
| **Iceberg** | `Parameters["table_type"] = "ICEBERG"` 加 `Parameters["metadata_location"]` 指向 S3 上的 Iceberg 元数据 JSON 文件。`StorageDescriptor.Columns` 通常为空。增长迅速。 |

完整的 Glue 集成必须处理这两种表格式。

### 2.2 查询引擎如何使用 Glue

Trino 和 Spark 都有原生 Glue 支持 — 直接调用 AWS Glue SDK，不走 HMS Thrift：

| 引擎 | Hive 表 on Glue | Iceberg 表 on Glue |
|---|---|---|
| **Trino** | Hive connector + `hive.metastore=glue` | Iceberg connector + `iceberg.catalog.type=glue` |
| **Spark** | Hive catalog + `AWSGlueDataCatalogHiveClientFactory` | Iceberg catalog + `catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog` |

两个引擎都采用**一个 catalog 对应一个 connector** 的模型 — 单个 catalog 只处理 Hive 格式或 Iceberg 格式的表，不会同时处理两种。这与 Gravitino 现有的 catalog 模型一致。

### 2.3 Gravitino 当前架构

Gravitino 的 catalog 插件系统提供：
- **Hive catalog**（`provider=hive`）：通过 Thrift 连接 HMS。调用链：`HiveCatalogOperations` → `CachedClientPool` → `HiveClientImpl` → `HiveShimV2/V3` → `IMetaStoreClient`。
- **Iceberg catalog**（`provider=lakehouse-iceberg`）：支持可插拔后端（`catalog-backend=hive|jdbc|rest|memory|custom`），每种后端对应不同的 Iceberg `Catalog` 实现。
- **Trino/Spark connector**：属性转换器将 Gravitino catalog 属性翻译为引擎特定属性。

## 3. 设计方案对比

### 方案 A：新建 `catalog-glue` 模块

创建独立的 `catalogs/catalog-glue/`，包含自己的 `GlueCatalogOperations`、类型转换器和实体类，直接调用 AWS Glue SDK 处理 Hive 和 Iceberg 两种表。

**优点**：完全控制 Glue 特定行为。单个 catalog 处理混合表格式。
**缺点**：
- 重复 Hive catalog 中已有的逻辑（类型转换、分区处理、SerDe 解析）和 Iceberg catalog 中已有的逻辑（schema 转换、元数据加载）。
- Trino/Spark 集成需要"复合 Connector"，根据表类型路由查询 — 架构改动巨大。
- 实现面更大，维护负担更重。

### 方案 B：Glue 作为 Metastore 类型（采纳）

扩展现有 Hive 和 Iceberg catalog，将 Glue 作为后端选项。

**优点**：
- 复用全部现有 catalog 逻辑、类型转换、属性处理和实体模型。
- Trino/Spark 集成几乎零成本 — 两个引擎都已原生支持 Glue。
- 改动面小得多（修改约 15 个文件 + 1 个新文件，对比方案 A 约 15 个新文件）。
- 与 Trino 和 Spark 的 Glue 建模方式一致（作为 metastore 变体，而非独立 catalog 类型）。

**缺点**：
- 用户需要创建两个 Gravitino catalog 才能覆盖同一 Glue Data Catalog 中的 Hive 和 Iceberg 表。
- 无法添加 Glue 专有特性（如 Glue Crawler），除非扩展通用接口。

**决策**：选择方案 B — 复用优势和 Trino/Spark 对齐的收益超过两个 catalog 带来的轻微用户体验成本。

## 4. 详细设计

### 4.1 配置属性

Gravitino 已在 `S3Properties.java` 中定义了标准化的 AWS/S3 属性：

| 已有属性 | 用途 |
|---|---|
| `s3-access-key-id` / `s3-secret-access-key` | Iceberg、Hive（S3 存储 + Glue 认证） |
| `s3-region` | Iceberg、Hive（S3 存储 + Glue 区域） |
| `s3-role-arn` / `s3-external-id` | Iceberg、Hive（STS AssumeRole） |
| `s3-endpoint` | Iceberg、Hive（自定义 S3 端点） |

我们**复用 `s3-region` 作为 Glue 和 S3 的默认 AWS 区域**，**复用 `s3-access-key-id` / `s3-secret-access-key` 进行认证**。这些属性已存在于 `S3Properties.java` 中，Hive 和 Iceberg catalog 均已处理 — 无需新增凭证相关代码。

仅需两个新的 Glue 特定属性（使用 `aws-glue-` 前缀，以明确标识为 AWS Glue Data Catalog 专属配置，与通用的 `s3-` 存储属性区分）：

| 新属性 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `aws-glue-catalog-id` | 否 | 调用者的 AWS 账号 ID | Glue catalog ID。用于跨账号访问。 |
| `aws-glue-endpoint` | 否 | AWS 默认区域端点 | 自定义 Glue 端点 URL（用于 VPC 端点或 LocalStack 测试）。 |

无需其他 Glue 特定属性 — 所有认证和区域设置均由现有 `s3-*` 属性覆盖。

**认证优先级**（`S3Properties` 中已有实现，原样复用）：静态凭证（`s3-access-key-id` + `s3-secret-access-key`） → STS AssumeRole（`s3-role-arn`） → 默认凭证链（环境变量、实例配置文件）。`s3-*` 属性到 AWS SDK / Glue SDK 凭证的映射在属性转换层完成（见 4.2 和 4.3 节）。

### 4.2 Iceberg Catalog + Glue 后端

在 `IcebergCatalogBackend` 枚举中添加 `GLUE` 值，使用 Iceberg 内置的 `org.apache.iceberg.aws.glue.GlueCatalog`。

#### 数据流

```
用户输入: catalog-backend=glue, warehouse=s3://..., s3-region=us-east-1
  → IcebergCatalogOperations.initialize()
    → IcebergCatalogUtil.loadCatalogBackend(GLUE, config)
      → loadGlueCatalog(config)
        → new GlueCatalog().initialize("glue", {
            "warehouse": "s3://...",
            "client.region": "us-east-1",
            "glue.catalog-id": "..." })
  → 所有现有 IcebergCatalogOperations 方法无需修改
```

`GlueCatalog` 是 Iceberg 官方实现，完整支持 Schema CRUD + Table CRUD — 这是本设计中风险最低的部分。

#### 引擎集成

**Trino** — 在 `IcebergCatalogPropertyConverter.gravitinoToEngineProperties()` 中添加 `case "glue":` 分支：

```java
// IcebergCatalogPropertyConverter.java 中：
case "glue":
  icebergProperties.put("iceberg.catalog.type", "glue");
  String region = properties.get("s3-region");
  if (region != null) {
    icebergProperties.put("hive.metastore.glue.region", region);
  }
  String catalogId = properties.get("aws-glue-catalog-id");
  if (catalogId != null) {
    icebergProperties.put("hive.metastore.glue.catalogid", catalogId);
  }
  break;
```

**Spark** — 无需代码修改。现有的 `IcebergPropertiesConverter` 通用透传：`all.put(ICEBERG_CATALOG_TYPE, catalogBackend)` 会将 `"glue"` 传递给 Spark 的 Iceberg catalog，后者原生支持 `GlueCatalog`。

### 4.3 Hive Catalog + Glue 后端

添加 `metastore-type=glue` 属性（Gravitino 用户侧 key）。在 `HiveCatalogOperations.initialize()` 中，通过 `GRAVITINO_CONFIG_TO_HIVE` 映射为 Hive 内部属性 `metastore.type=glue`。以下 Java 代码片段均使用 Hive 内部 key `metastore.type`。使用 AWS 开源的 `aws-glue-datacatalog-hive3-client` 库，该库提供基于 Glue SDK 的 `IMetaStoreClient` 实现。

#### 数据流

```
用户输入: metastore-type=glue, s3-region=us-east-1
  → HiveCatalogOperations.initialize()
    → mergeProperties(conf) — 映射 Glue 属性
    → CachedClientPool(properties)
      → HiveClientPool.newClient()
        → HiveClientFactory.createHiveClient()      ← 修改：跳过 hive2/3 探测
          → HiveClientClassLoader.createLoader(HIVE3, ...)  ← Glue 始终用 Hive3
          → HiveClientImpl(HIVE3, properties)
            → 检测到 metastore.type=glue
            → new GlueShim(properties)               ← 新增（替代 HiveShimV3）
              → createMetaStoreClient()
                → AWSGlueDataCatalogHiveClientFactory.create(hiveConf)
                → 返回 AWSCatalogMetastoreClient（实现 IMetaStoreClient）
  → 所有现有 HiveCatalogOperations 方法无需修改
```

#### Hive 版本决策

**问题**：`HiveClientFactory.createHiveClientWithBackend()` 当前通过运行时探测远端 HMS 来检测 Hive2 还是 Hive3 — 调用 `getCatalogs()`，失败则回退到 Hive2。这种探测回退方式对 Glue 有两个问题：(1) 没有远端 HMS 可探测，(2) 版本可从 catalog 配置直接确定。

**方案**：提取 `resolveHiveVersion(Properties)` 方法，根据 catalog 配置确定 Hive 版本，避免不必要的运行时探测：

```java
// HiveClientFactory 中：

/**
 * 根据 catalog 配置确定 Hive 版本。
 * 无法静态确定时（HMS 模式）返回 UNKNOWN。
 */
private HiveVersion resolveHiveVersion(Properties properties) {
  String metastoreType = properties.getProperty("metastore.type", "hive");
  if ("glue".equalsIgnoreCase(metastoreType)) {
    return HiveVersion.HIVE3;  // Glue 始终使用 Hive3
  }
  return HiveVersion.UNKNOWN;  // HMS：由 createHiveClient() 运行时探测
}
```

当 `resolveHiveVersion()` 返回 `UNKNOWN` 时，`createHiveClient()` 进入现有的探测回退路径（`createHiveClientWithBackend()`）。当返回具体版本（`HIVE3`）时，完全跳过探测。

此设计：
- **消除硬编码**：版本决策集中在一个方法中，由 catalog 配置驱动。
- **可扩展**：未来新增后端只需在 `resolveHiveVersion()` 中增加分支，无需修改 `createHiveClient()`。
- **保持现有行为**：对于 HMS metastore（`metastore.type=hive` 或未设置），现有探测回退逻辑不变。

**为什么 Glue 用 Hive3？** AWS Glue Data Catalog 是托管服务，只有一个 API 版本 — 服务端不存在 Hive2/Hive3 的概念。选择 Hive3 classloader 的原因：
1. **JAR 路径**：`HiveClientClassLoader.getJarDirectory()` 将 `HIVE3` 映射到 `hive-metastore3-libs/`，Glue 客户端 JAR 放在此目录（见 4.4 节）。
2. **持续维护**：AWS 的 `aws-glue-datacatalog-hive3-client` 是活跃维护的版本，Hive2 客户端为遗留版本。
3. **API 兼容性**：`IMetaStoreClient` 接口在 Hive2 和 Hive3 间有差异（Hive3 增加了 catalog-aware 方法）。Glue 客户端 JAR 必须匹配其所在 classloader 的 Hive 版本。

#### GlueShim 设计

`GlueShim` 继承 `HiveShimV3`，仅覆写 `createMetaStoreClient()`：

| Shim | 父类 | `createMetaStoreClient()` | 调用约定 |
|---|---|---|---|
| `HiveShimV2` | `HiveShim` | `RetryingMetaStoreClient.getProxy(hiveConf)` → Thrift HMS | 2 参数：`getDatabase(db)` |
| `HiveShimV3` | `HiveShimV2` | 与 V2 相同 | 3 参数：`getDatabase(catalog, db)` — catalog-aware |
| `GlueShim` | `HiveShimV3` | `AWSGlueDataCatalogHiveClientFactory.create(hiveConf)` → Glue SDK | 继承 HiveShimV3 的 3 参数约定 |

**为什么继承 `HiveShimV3`？** GlueShim 使用 Hive3 classloader 和 `aws-glue-datacatalog-hive3-client`，后者实现的是 Hive3 版本的 `IMetaStoreClient`。`HiveShimV3` 提供了与此接口匹配的 3 参数调用约定（catalog-aware 方法签名）。如果继承 `HiveShimV2`，将使用 2 参数约定，与 Hive3 classloader 加载的 `IMetaStoreClient` 不匹配。

三者都返回 `IMetaStoreClient`。`HiveClientImpl` 根据 `metastore.type` 选择 shim：

```java
// HiveClientImpl 构造函数中：
String metastoreType = properties.getProperty("metastore.type", "hive");
if ("glue".equalsIgnoreCase(metastoreType)) {
  shim = new GlueShim(properties);   // 继承 HiveShimV3
} else {
  switch (hiveVersion) {
    case HIVE2: shim = new HiveShimV2(properties); break;
    case HIVE3: shim = new HiveShimV3(properties); break;
  }
}
```

所有上游代码（`HiveClientPool`、`CachedClientPool`、`HiveCatalogOperations`）无需修改 — 它们面向 `HiveClient` 接口编程。

#### IMetaStoreClient 关系

```
org.apache.hadoop.hive.metastore.IMetaStoreClient    ← Hive 标准接口
    ├── HiveMetaStoreClient（Thrift 实现，连接 HMS）
    └── AWSCatalogMetastoreClient（Glue 实现，通过 AWS Glue SDK）
         └── 由 AWSGlueDataCatalogHiveClientFactory.create(hiveConf) 创建
```

`AWSCatalogMetastoreClient` 是 `HiveMetaStoreClient` 的直接替代品。所有上游代码完全无感知差异。

#### 引擎集成

**Trino** — 在 `HiveConnectorAdapter.buildInternalConnectorConfig()` 中添加 Glue 分支：

```java
// HiveConnectorAdapter.java 中：
String metastoreType = catalog.getProperty("metastore-type", "hive");
if ("glue".equalsIgnoreCase(metastoreType)) {
  config.put("hive.metastore", "glue");
  String region = catalog.getProperty("s3-region");
  if (region != null) {
    config.put("hive.metastore.glue.region", region);
  }
  String catalogId = catalog.getProperty("aws-glue-catalog-id");
  if (catalogId != null) {
    config.put("hive.metastore.glue.catalogid", catalogId);
  }
} else {
  config.put("hive.metastore.uri", catalog.getRequiredProperty("metastore.uris"));
}
```

**Spark** — 在 `HivePropertiesConverter.toSparkCatalogProperties()` 中添加 Glue 分支：

```java
// HivePropertiesConverter.java 中：
String metastoreType = properties.get("metastore-type");
if ("glue".equalsIgnoreCase(metastoreType)) {
  sparkProperties.put("spark.hadoop.hive.metastore.client.factory.class",
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");
  String region = properties.get("s3-region");
  if (region != null) {
    sparkProperties.put("spark.hadoop.aws.region", region);
  }
} else {
  sparkProperties.put(SPARK_HIVE_METASTORE_URI, properties.get(GRAVITINO_HIVE_METASTORE_URI));
}
```

### 4.4 依赖管理

#### Iceberg + Glue

| 依赖 | 目标模块 | 作用域 |
|---|---|---|
| `org.apache.iceberg:iceberg-aws` — 包含 `GlueCatalog` 实现，传递依赖 `software.amazon.awssdk:glue`。已在版本目录中定义为 `libs.iceberg.aws`。 | `iceberg/iceberg-common/build.gradle.kts` | `compileOnly`（运行时由 `bundles/iceberg-aws-bundle` 提供） |

无需修改 `gradle/libs.versions.toml`。

#### Hive + Glue

| 依赖 | 目标模块 | 作用域 |
|---|---|---|
| `com.amazonaws:aws-glue-datacatalog-hive3-client` — 通过 Glue SDK 实现 `IMetaStoreClient`，提供 `AWSGlueDataCatalogHiveClientFactory`。 | `catalogs/hive-metastore3-libs/build.gradle.kts` | `implementation`（打包到 `hive-metastore3-libs/` 目录） |

**为什么放在 `hive-metastore3-libs`？** 因为 Hive catalog 使用 `HiveClientClassLoader` 进行类隔离 — 它从 `hive-metastore2-libs/` 或 `hive-metastore3-libs/` 目录加载 JAR。GlueShim 使用 Hive3 classloader（见 4.3 节），因此 Glue 客户端 JAR 必须放在 `hive-metastore3-libs` 中。

### 4.5 端到端架构

```
                        Gravitino Server
                              |
            +------ provider=hive ------+------- provider=lakehouse-iceberg ------+
            |     metastore-type=glue   |         catalog-backend=glue            |
            |                           |                                         |
     HiveCatalogOperations       IcebergCatalogOperations
            |                           |
     HiveClientImpl                   IcebergCatalogUtil
     -> GlueShim                      -> loadGlueCatalog()
     -> AWSCatalogMetastoreClient     -> org.apache.iceberg.aws.glue.GlueCatalog
     (impl IMetaStoreClient)          (impl org.apache.iceberg.catalog.Catalog)
            |                           |
            +-------- AWS Glue SDK -----+
                          |
                  AWS Glue Data Catalog
                          |
               +----------+----------+
               |                     |
          Hive 表               Iceberg 表
     (StorageDescriptor)    (metadata_location)


                        查询引擎
                              |
        +---- Trino ----+               +---- Spark ----+
        |               |               |               |
   Hive Connector  Iceberg Connector  HiveCatalog  SparkCatalog
   metastore=glue  catalog.type=glue  factory=AWS  catalog-impl=GlueCatalog
```

## 5. 测试策略

### 5.1 单元测试

**属性转换** — 扩展现有测试类：

| 测试类 | 新增测试用例 |
|---|---|
| `TestIcebergCatalogPropertyConverter` | `testGlueBackendProperty()`、`testGlueBackendMissingWarehouse()` |
| `TestHiveConnectorAdapter` | `testBuildGlueConfig()`、`testBuildGlueConfigWithCatalogId()` |

**Hive 客户端路由** — 新建 `TestHiveClientImpl`（位于 `hive-metastore-common/src/test/`）：
- `testGlueShimSelection()`：验证 `metastore.type=glue` 时创建 `GlueShim`
- `testDefaultHiveShimSelection()`：验证 `metastore.type=hive` 或未设置时创建 `HiveShimV2/V3`
- `testHiveClientFactorySkipsProbeForGlue()`：验证直接使用 Hive3 classloader

**GlueShim** — 新建 `TestGlueShim`（位于 `hive-metastore-common/src/test/`）：
- `testCreateMetaStoreClient()`：mock factory，验证正确调用
- `testGlueShimExtendsHiveShimV3()`：验证继承关系
- `testGluePropertiesPassedToHiveConf()`：验证属性传递

### 5.2 集成测试 — 复用现有测试框架

项目拥有成熟的集成测试继承体系。Glue 测试继承现有父类 — 仅覆写环境初始化。

**Catalog 操作**：

| 新测试类 | 继承 | 覆写 |
|---|---|---|
| `CatalogHiveGlueIT` | `CatalogHive3IT`（23 个测试） | `startNecessaryContainer()` → LocalStack；`createCatalogProperties()` → `metastore-type=glue` |
| `CatalogIcebergGlueIT` | `CatalogIcebergBaseIT`（15 个测试） | `initIcebergCatalogProperties()` → `catalog-backend=glue` |

示例：
```java
@Tag("gravitino-docker-test")
public class CatalogHiveGlueIT extends CatalogHive3IT {
  @Override
  protected void startNecessaryContainer() {
    containerSuite.startLocalStackContainer();
  }

  @Override
  protected Map<String, String> createCatalogProperties() {
    return ImmutableMap.of(
        "metastore-type", "glue",
        "s3-region", "us-east-1",
        "aws-glue-endpoint", localStackEndpoint);
  }
}
```

父类所有测试（`testCreateHiveTable`、`testAlterTable`、`testListPartitions` 等）自动在 Glue 后端运行 — 无需重写。

**Hive + Glue 支持的操作**（全部由继承的 `CatalogHive3IT` 测试覆盖）：

- Schema：`createDatabase`、`getDatabase`、`getAllDatabases`、`alterDatabase`、`dropDatabase(cascade)`
- Table：`createTable`、`getTable`、`getAllTables`、`alterTable`、`dropTable(deleteData)`、`purgeTable`、`getTableObjectsByName`
- Partition：`listPartitionNames`、`listPartitions`、`listPartitions(filter)`、`getPartition`、`addPartition`、`dropPartition(deleteData)`

这些操作均由 `AWSCatalogMetastoreClient`（`IMetaStoreClient`）直接支持。GlueShim 仅创建客户端实例 — 上游 `HiveShim` 方法自动工作。

**Trino E2E**：在现有 testset 目录中添加 Glue catalog SQL 文件 — 无需修改 Java 代码：

```
trino-ci-testset/testsets/
  hive/
    catalog_hive_glue_prepare.sql      ← 新增：创建 Glue 后端 Hive catalog
    catalog_hive_glue_cleanup.sql      ← 新增：删除 Glue 后端 Hive catalog
    （现有 *.sql 测试脚本自动复用）
  lakehouse-iceberg/
    catalog_iceberg_glue_prepare.sql   ← 新增：创建 Glue 后端 Iceberg catalog
    catalog_iceberg_glue_cleanup.sql   ← 新增：删除 Glue 后端 Iceberg catalog
    （现有 *.sql 测试脚本自动复用）
```

示例 `catalog_hive_glue_prepare.sql`：
```sql
call gravitino.system.create_catalog(
    'gt_hive_glue', 'hive',
    map(
        array['metastore-type', 's3-region', 'aws-glue-endpoint'],
        array['glue', 'us-east-1', '${glue_endpoint}']
    )
);
```

各 testset 目录中的现有 SQL 测试脚本自动在新 Glue 后端 catalog 上运行。

**Spark E2E**：

| 新测试类 | 继承 | 覆写 |
|---|---|---|
| `SparkHiveGlueCatalogIT` | `SparkHiveCatalogIT` | `getCatalogConfigs()` → Glue 配置 |
| `SparkIcebergGlueCatalogIT` | `SparkIcebergCatalogIT` | catalog 属性 → `catalog-backend=glue` |

`SparkCommonIT` 的所有测试（31 个 DDL/DML/查询测试）自动继承。

### 5.3 构建验证

```bash
# 编译所有修改的模块
./gradlew :iceberg:iceberg-common:build -x test
./gradlew :catalogs:catalog-hive:build -x test
./gradlew :catalogs:hive-metastore-common:build -x test
./gradlew :catalogs:hive-metastore3-libs:build -x test
./gradlew :trino-connector:trino-connector:build -x test
./gradlew :spark-connector:spark-common:build -x test

# 修改模块的单元测试
./gradlew :iceberg:iceberg-common:test -PskipITs
./gradlew :catalogs:hive-metastore-common:test -PskipITs
./gradlew :trino-connector:trino-connector:test -PskipITs

# 集成测试（Docker + LocalStack）
./gradlew :catalogs:catalog-hive:test -PskipTests -PskipDockerTests=false \
  --tests "*.CatalogHiveGlueIT"
./gradlew :catalogs:catalog-lakehouse-iceberg:test -PskipTests -PskipDockerTests=false \
  --tests "*.CatalogIcebergGlueIT"
```

## 6. 实施计划

### 第一阶段：完整 Glue 后端支持（实现所有现有接口）

第一阶段的目标是让 Glue 后端通过所有现有集成测试。Hive catalog 实现了 `SupportsSchemas` + `TableCatalog`（含分区操作），Iceberg catalog 实现了 `SupportsSchemas` + `TableCatalog`。所有方法必须完整实现 — 不是只读。

#### WI-1：Iceberg + Glue 后端

无需实现新接口。Apache Iceberg 的 `GlueCatalog`（来自 `iceberg-aws`）已完整实现 `Catalog` 接口 — 包括 Schema CRUD（`listNamespaces`、`createNamespace`、`loadNamespaceMetadata`、`setProperties`、`dropNamespace`）和 Table CRUD（`listTables`、`createTable`、`loadTable`、`renameTable`、`dropTable`）。本工作项将 Gravitino 的配置层与 `GlueCatalog` 实例化对接。

| # | 工作项 | 涉及文件 | 说明 |
|---|---|---|---|
| 1.1 | 添加 `GLUE` 枚举值 | `IcebergCatalogBackend.java` | 在枚举中新增 `GLUE` |
| 1.2 | 添加 Glue 属性常量 | `IcebergConstants.java` | `GLUE_CATALOG_ID`、`GLUE_ENDPOINT` |
| 1.3 | 添加属性映射 | `IcebergPropertiesUtils.java` | `s3-region` → `client.region`、`aws-glue-catalog-id` → `glue.catalog-id` 等 |
| 1.4 | 声明属性元数据 | `IcebergCatalogPropertiesMetadata.java` | 注册 `aws-glue-catalog-id`、`aws-glue-endpoint` |
| 1.5 | 添加 ConfigEntry | `IcebergConfig.java` | Glue 相关配置项定义 |
| 1.6 | 实现 `loadGlueCatalog()` | `IcebergCatalogUtil.java` | `case GLUE:` 分支，实例化并初始化 `GlueCatalog` |
| 1.7 | 添加依赖 | `iceberg/iceberg-common/build.gradle.kts` | `compileOnly(libs.iceberg.aws)` |
| 1.8 | 单元测试 | `TestIcebergCatalogPropertyConverter.java` | `testGlueBackendProperty()`、`testGlueBackendMissingWarehouse()` |

#### WI-2：Hive + Glue 后端

无需实现新的 catalog 接口。AWS 的 `AWSCatalogMetastoreClient`（来自 `aws-glue-datacatalog-hive3-client`）已完整实现 `IMetaStoreClient` 接口 — 包括所有 Schema、Table 和 Partition 操作：

- Schema：`createDatabase`、`getDatabase`、`getAllDatabases`、`alterDatabase`、`dropDatabase(cascade)`
- Table：`createTable`、`getTable`、`getAllTables`、`alterTable`、`dropTable(deleteData)`、`purgeTable`、`getTableObjectsByName`
- Partition：`listPartitionNames`、`listPartitions`、`listPartitions(filter)`、`getPartition`、`addPartition`、`dropPartition(deleteData)`

本工作项创建一个薄路由层（`GlueShim` — 仅覆写一个方法），将 `AWSCatalogMetastoreClient` 插入现有 Hive 客户端链。所有上游代码（`HiveShim`、`HiveClientImpl`、`HiveCatalogOperations`）通过 `IMetaStoreClient` / `HiveClient` 接口无需修改。

| # | 工作项 | 涉及文件 | 说明 |
|---|---|---|---|
| 2.1 | 添加 Hive 常量 | `HiveConstants.java` | `METASTORE_TYPE`、`METASTORE_TYPE_GLUE`、`AWS_GLUE_CATALOG_ID`、`AWS_GLUE_ENDPOINT` |
| 2.2 | 声明属性元数据 | `HiveCatalogPropertiesMetadata.java` | 注册 `metastore-type` + Glue 属性；`metastore.uris` 改为条件必填 |
| 2.3 | 属性映射 | `HiveCatalogOperations.java` | `GRAVITINO_CONFIG_TO_HIVE` 增加 Glue 属性；`initialize()` 增加 Glue 分支 |
| 2.4 | 新增 `resolveHiveVersion()` | `HiveClientFactory.java` | 提取版本决策方法；Glue → HIVE3 无需探测 |
| 2.5 | 修改 HiveClientImpl | `HiveClientImpl.java` | `metastore.type=glue` 时选择 `GlueShim` |
| 2.6 | 创建 GlueShim | `GlueShim.java`（新文件） | 继承 `HiveShimV3`，覆写 `createMetaStoreClient()` 使用 `AWSGlueDataCatalogHiveClientFactory` |
| 2.7 | 添加依赖 | `hive-metastore3-libs/build.gradle.kts` | `implementation("com.amazonaws:aws-glue-datacatalog-hive3-client:...")` |
| 2.8 | 单元测试 | `TestHiveClientImpl.java`（新）、`TestGlueShim.java`（新） | 路由逻辑、GlueShim |

#### WI-3：集成测试（Catalog 操作）

| # | 工作项 | 涉及文件 | 说明 |
|---|---|---|---|
| 3.1 | LocalStack 容器支持 | `ContainerSuite.java` | 添加 `startLocalStackContainer()` |
| 3.2 | Hive + Glue 集成测试 | `CatalogHiveGlueIT.java`（新） | 继承 `CatalogHive3IT`，使用 LocalStack |
| 3.3 | Iceberg + Glue 集成测试 | `CatalogIcebergGlueIT.java`（新） | 继承 `CatalogIcebergBaseIT` |

#### WI-4：Trino 和 Spark Connector 支持

| # | 工作项 | 涉及文件 | 说明 |
|---|---|---|---|
| 4.1 | Iceberg — Trino 属性转换 | `IcebergCatalogPropertyConverter.java` | `case "glue":` → `iceberg.catalog.type=glue` |
| 4.2 | Hive — Trino 属性转换 | `HiveConnectorAdapter.java` | `metastore-type=glue` → `hive.metastore=glue` |
| 4.3 | Hive — Spark 属性转换 | `HivePropertiesConverter.java` | 设置 `hive.metastore.client.factory.class` |
| 4.4 | 单元测试 | `TestIcebergCatalogPropertyConverter.java`、`TestHiveConnectorAdapter.java` | Trino Glue 配置测试 |
| 4.5 | Trino + Glue E2E | `trino-ci-testset/testsets/hive/catalog_hive_glue_*.sql`、`trino-ci-testset/testsets/lakehouse-iceberg/catalog_iceberg_glue_*.sql` | 在 testsets 目录添加 Glue catalog 准备/清理 SQL |
| 4.6 | Spark + Hive + Glue E2E | `SparkHiveGlueCatalogIT.java`（新） | 继承 `SparkHiveCatalogIT` |
| 4.7 | Spark + Iceberg + Glue E2E | `SparkIcebergGlueCatalogIT.java`（新） | 继承 `SparkIcebergCatalogIT` |

Iceberg — Spark 无需代码修改（通用透传）。

#### WI-5：文档与 CI

| # | 工作项 | 说明 |
|---|---|---|
| 5.1 | 更新 Gravitino 用户文档 | 在 Hive catalog 和 Iceberg catalog 文档中添加 Glue 配置示例 |
| 5.2 | CI 配置 | 确保 Glue 集成测试在 CI 中运行（需要 LocalStack 容器） |

---

### 第二阶段：高级特性（后续迭代）

| # | 工作项 | 说明 | 优先级 |
|---|---|---|---|
| P2-1 | **跨账号 Glue 访问** | 支持 `aws-glue-catalog-id` + STS AssumeRole（`s3-role-arn`）跨账号访问 | 高 |
| P2-2 | **Glue 表过滤** | Hive catalog 注册时过滤 Iceberg 表（`Parameters["table_type"]="ICEBERG"` 的表不显示），避免用户混淆 | 中 |
| P2-3 | **VPC 端点支持** | 验证 `aws-glue-endpoint` 在 VPC 私有链路场景下的支持，添加集成测试 | 中 |
| P2-4 | **Paimon / Hudi on Glue** | 将 Glue 支持扩展到 Paimon、Hudi 等其他 lakehouse 格式 | 低 |
