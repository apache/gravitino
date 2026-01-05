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

# Trino Connector 多版本支持方案

## 背景
Gravitino Trino-connector 过去主要支持 Trino 435–439 版本段，与社区最新的 Trino 版本相比已经存在一定滞后（截至 2024/2025 年已至 479+）。

随着社区和用户逐步升级，SPI/API/JDK 之间的差异迅速放大（如 JDK 17→21→22→24，SPI 方法签名频繁变更），
单一 artifact 跨多个大版本兼容会带来严重的运行时风险，并显著增加开发和测试负担。

因此，我们采用“按 Trino 版本段划分、多模块独立适配和发布”的模式，以持续覆盖主流（包含长期维护版和社区最新版本）的 Trino 生态，兼顾兼容性与工程可维护性。

## 目标

- **覆盖范围**：支持 Trino **>= 435**，并以“版本段”方式（如 435–439、440、455、478…）提供兼容性保障。
- **构建方式**：支持通过构建参数/任务选择目标版本段，产出对应的 connector 插件包（artifact）。
- **演进成本**：新增一个版本段的支持应当是“低成本、可控变更”（差异点集中在少量覆盖类/适配层，避免大面积复制）。
- **稳定性**：保证发布、部署与回归测试流程清晰可维护。

### 方案概述

#### 多模块分层结构

Trino 的 SPI 在部分版本范围内相对稳定，例如使用 trino-spi-435 到 439 时接口较为一致。基于这些稳定的 SPI 版本，将 trino-connector 划分为公共模块和特定版本段模块。

- `trino-connector-common`：承载稳定接口、共享逻辑（只依赖 Trino 官方稳定 SPI/API），存放绝大多数通用逻辑、通用测试基类；
- `trino-connector-435-439/440-455/456-469/470-478/...`：每个版本段一个子模块，依赖自己目标段对应的 Trino SPI/JDK。

目录结构示例：

```text
trino-connector/
 ├── trino-connector-common/       # 核心与共性逻辑、测试基类
 ├── trino-connector-435-439/      # 435–439 版本段（举例）
 ├── trino-connector-440-455/      # 440–455 版本段
 ├── trino-connector-456-469/      # 456–469 版本段
 └── trino-connector-470-478/      # 470–478 版本段
```

版本段的数量和粒度可根据社区实际兼容需求进行增减和调整。

每个版本段模块：
- 仅实现本段特有的差异点，例如 SPI 签名变更、新增/删除方法、类加载或反射相关 hack；
- 测试层面补充本段特有的 UT，共性逻辑优先沉入 common 层测试；
- 对于 SPI 相对稳定的版本段，一般以该版本段的首个版本作为该模块使用的 Trino 依赖版本。


## API适配

各段变化集中但不唯一，常见差异点归纳：
- `setNodeCount` -> `setWorkerCount`（QueryRunner，JDK21以后）
- `getNextPage` -> `getNextSourcePage` / `SourcePage` 类型变化
- `finishInsert` / `finishMerge` 新签名，需要 unwrap `sourceTableHandles`
- `getSplitBucketFunction` 新增 `bucketCount` 参数
- `addColumn` 新增 `ColumnPosition` 参数
- `ConnectorSplit.getInfo` SPI 移除
- `Connector.shutdown` 生命周期接口变化

**适配原则：**
- 差异点务必集中于控制层（例如适配器、覆写类、桥接类），避免在代码各处零散出现，造成维护成本失控；
- 若兼容层必须使用反射等 hack 手段，应将影响控制在最小范围，在主路径代码中尽量避免；
- 每个受维护版本段的代码都必须保持可编译、测试通过，以避免新增改动破坏既有兼容性。

## 构建与打包

### JDK 依赖

下表给出了各 Trino 版本段推荐的编译/运行 JDK 及 classfile 版本：

| 版本段     | JDK版本 | Classfile |
|---------|-------|-----------|
| 435–439 | 17    | 61        |
| 440–446 | 21    | 65        |
| 447–469 | 22    | 66        |
| 470–478 | 24    | 68        |


**工程配置要点：**

- 支持通过 Gradle 参数化构建，例如使用 `-PtrinoVersions=435-449,470–478` 控制需要构建的 Trino connector 版本段；
- 项目 build 脚本 `build.gradle.kts` 能为目标版本段准确设定 JDK 版本；
- Toolchain 兼容性会受到 JDK 版本影响（如 ErrorProne、JaCoCo、JUnit），需要按需升级以适配对应 JDK 版本；
- 通过 Gradle 任务（如 `./gradlew :assembleDistribution -PtrinoVersions=435-439,470–478`）可以单独构建一个或多个版本段的插件包；
  生成的 jar 位于对应子模块的 `build/libs/` 目录下。

```text
distribution/gravitino-trino-connector-435-439-1.1.2.tar.gz
distribution/gravitino-trino-connector-470-478-1.1.2.tar.gz
```

最终只需将对应压缩包文件解压后，放到 Trino 的 `${TRINO_HOME}/plugin/gravitino/` 目录下，重启即可生效。

## 测试与CI

### 单元测试
- 各版本段都需要 UT 覆盖，共性测试逻辑放在 `trino-connector-common` 中；
- 每次 PR/Release 需要以矩阵方式测试所有受支持的版本段，以及每个版本段的首尾版本。

### 集成测试
- 改造现有 Docker Compose 测试环境，实现多版本 Trino 的自动集成测试；
- 每次 PR 至少测试最新版本段的最后一个版本；
- 每次 Release 测试每个版本段的首尾两个版本。


## 打包/发布

Release 发布时，可以采用两种方案：
1. 只发布最新版本段的 artifact；
2. 发布最近一段时间内仍在支持范围内的多个版本段 artifact。

只发布最新版本段的 artifact 可以简化用户选择，维护和测试成本也相对较低；但如果用户仍在使用较老版本段的 Trino，就无法直接下载到对应版本的
Gravitino Trino connector，需要自行编译旧版本段的 trino-connector。

发布最近 n 年（可根据支持策略调整，建议为 2 年，通常用户选定 Trino 集群版本后不会高频升级）的版本段 artifact，可以覆盖更多用户场景，但会显著增加维护和测试成本。



### 发布策略建议
- 推荐每个 release 尽可能覆盖所有仍在维护的主流版本段包（除非已与 Trino 社区明确约定放弃某些版本段）；
- 如维护成本受限，可优先主推最新版本段（例如 479 段），对老版本段进行梳理，仅对长期 LTS 版本保持持续支持；对于更老的版本段，仅保留源码供用户自行构建。

### 用户选择指引

用户可根据实际使用的 Trino 版本选择对应的插件包：

| Trino 版本范围  | 
| -------------- |
| 435–439        |
| 440–455        |
| 470–478        |

对于 Trino 478 以上的版本，建议优先选择最新版本段的插件包。
如遇新版本或冷门版本尚未被正式标记为支持，也可以先选择版本号相近的版本段，并通过配置参数 `gravitino.trino.skip-version-validation=true` 临时跳过版本校验，以便进行验证测试。
