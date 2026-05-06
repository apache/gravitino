# Spark Glue Connector 集成测试修复进度

> 更新于 2026-04-26

## 目标

为 Spark Glue 连接器实现并修复集成测试，验证以下场景：
- 混合表类型支持（Hive 格式 + Iceberg 格式在同一个 Glue 数据库中）
- 通过 Gravitino Spark 插件操作 Glue 元数据
- DDL（创建/删除/重命名/修改表）和 DML（INSERT/SELECT）操作

## 测试架构

### 类继承链

```
SparkGlueCatalogIT34/35 (v3.4/v3.5, concrete)
  └── SparkGlueCatalogIT (spark-common, abstract)  ← 测试用例
        └── SparkGlueEnvIT (spark-common, abstract) ← 环境配置
              └── SparkCommonIT (root base)
                    └── SparkEnvIT
                          └── SparkUtilIT → BaseIT

SparkAwsGlueCatalogIT34/35 (真实 AWS Glue, 默认跳过)
  └── SparkGlueCatalogIT
```

### 两套测试环境

| 测试类 | 环境 | 触发条件 |
|---|---|---|
| `SparkGlueCatalogIT34/35` | LocalStack（moto mock AWS） | 默认运行 |
| `SparkAwsGlueCatalogIT34/35` | 真实 AWS Glue + S3 | 设置 `AWS_ACCESS_KEY_ID` 环境变量 |

### LocalStack 测试配置
- **Glue Catalog API**: LocalStack 容器（port 4566），mock AWS Glue API
- **数据存储**: S3 bucket (`ice-glue-test-01`)，通过 LocalStack S3
- **Warehouse**: `s3a://ice-glue-test-01/warehouse`
- **Credentials**: test/test

### 真实 AWS 测试配置
- **Glue Catalog API**: 真实 AWS Glue（通过 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`）
- **数据存储**: S3 bucket（可配置，默认 `ice-glue-test-01`）
- **Region**: 可配置，默认 `us-east-1`

## 核心文件

| 文件 | 用途 |
|---|---|
| `spark-connector/spark-common/.../glue/SparkGlueEnvIT.java` | 环境配置：S3A、Glue endpoint、SparkConf |
| `spark-connector/spark-common/.../glue/SparkGlueCatalogIT.java` | Glue 测试用例 |
| `spark-connector/v3.4/.../glue/SparkGlueCatalogIT34.java` | Spark 3.4 + LocalStack |
| `spark-connector/v3.4/.../glue/SparkAwsGlueCatalogIT34.java` | Spark 3.4 + 真实 AWS |
| `spark-connector/v3.5/.../glue/SparkGlueCatalogIT35.java` | Spark 3.5 + LocalStack |
| `spark-connector/v3.5/.../glue/SparkAwsGlueCatalogIT35.java` | Spark 3.5 + 真实 AWS |
| `spark-connector/spark-common/.../util/SparkTableInfo.java` | 测试工具：提取表元数据（含 bug） |

## 当前状态

### 已解决 ✅

1. **`IdentityTransform` 崩溃**
   - **根因**: `SparkTableInfo.java:150` 的 else-if 没有处理 `IdentityTransform`，导致抛 `UnsupportedOperationException: Doesn't support Spark transform: identity`
   - **修复**: 添加 `|| transform instanceof IdentityTransform` 到 else-if 条件
   - **文件**: `spark-connector/spark-common/src/test/java/.../util/SparkTableInfo.java`
   - **影响**: 修复了 13 个测试的级联失败

### 待解决 🔴

| # | 测试 | 根因 | 状态 |
|---|---|---|---|
| 1 | `testLoadCatalogs` | glue catalog 未出现在 catalog 列表（metalake 初始化问题） | 🔴 未分析 |
| 2 | `testDropAndWriteTable` | `dropTableIfExists()` 未从 Spark session catalog 移除表 | 🔴 未分析 |
| 3 | `testCreateAndLoadSchema` | "default" schema 已存在 | 🔴 未分析 |
| 4 | `testCallUDF` / `testListFunctions` | `@EnabledIf("supportsFunction")` 引用不存在的方法 | 🔴 未分析 |
| 5 | `testInsertHiveTable` 数据格式错 | `getExpectedTableData()` 返回格式不符 | 🔴 未分析 |
| 6 | `testRenameTable` | 下游于 IdentityTransform 崩溃 | 🔴 待验证（IdentityTransform 修复后） |
| 7 | `testDropTable` | 下游于 IdentityTransform 崩溃 | 🔴 待验证 |
| 8 | `testAlterTableUpdateComment` | 下游于 IdentityTransform 崩溃 | 🔴 待验证 |
| 9 | `SparkGlueCatalogIT35` | 未运行 | 🔴 待运行 |

## 测试运行命令

```bash
# LocalStack 版本（默认）
./gradlew :spark-connector:spark-3.4:test --tests "*SparkGlueCatalogIT34" -PskipTests
./gradlew :spark-connector:spark-3.5:test --tests "*SparkGlueCatalogIT35" -PskipTests

# 真实 AWS 版本（需设置环境变量）
AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=yyy ./gradlew :spark-connector:spark-3.4:test --tests "*SparkAwsGlueCatalogIT34" -PskipTests

# 单元测试（快速验证，分钟级）
./gradlew :spark-connector:spark-common:test -PskipITs
```

## 关键设计说明

### Glue vs Hive Metastore

Glue 测试使用与 Hive 测试完全不同的基础设施：
- **Hive**: HMS（Hive Metastore）+ HDFS 存储
- **Glue**: AWS Glue Catalog API（mock 或真实）+ S3 存储

关键覆盖：
- `initHiveEnv()` → 空实现（跳过 HMS）
- `initHdfsFileSystem()` → 空实现（跳过 HDFS）
- `checkDirExists()` → S3A filesystem 检查
- `deleteDirIfExists()` → S3 delete
- `warehouse` → `s3a://bucket/warehouse`（非 HDFS path）
- `hiveMetastoreUri` → `null`

### S3A 配置逻辑（`SparkGlueEnvIT.initSparkEnv()`）

```java
// 有 endpoint → LocalStack/自定义 S3
if (s3Endpoint != null) {
  sparkConf.set("spark.hadoop.fs.s3a.endpoint", s3Endpoint);
  sparkConf.set("spark.hadoop.fs.s3a.path.style.access", "true");
  sparkConf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false");
}
// 无 endpoint → 真实 AWS（自动推导 endpoint）
else if (awsRegion != null) {
  sparkConf.set("spark.hadoop.fs.s3a.endpoint.region", awsRegion);
}
```

## 下一步计划

1. 运行 `spark-common` 单元测试（快速验证 IdentityTransform 修复无副作用）
2. 逐一分析剩余 6 个独立失败根因
3. 运行 `SparkGlueCatalogIT34` 集成测试验证修复
4. 运行 `SparkGlueCatalogIT35` 集成测试
5. 可选：运行真实 AWS 版本测试
