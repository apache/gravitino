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

# Trino Connector 多版本支持 TODO

## 工作项 1：验证共享 `trino-connector` 拆分方案

1. [x] 在 `trino-connector/trino-connector/build.gradle.kts` 中为 Trino 依赖声明统一的版本变量，后续子模块可通过 Gradle 属性覆盖。
2. [x] 在 `settings.gradle.kts` 注册 `:trino-connector:trino-connector-435-436`，并让其依赖现有 `:trino-connector:trino-connector`，同时在子模块 `build.gradle.kts` 中写死默认 435 版本（可通过属性覆盖）。
3. [x] 运行 `./gradlew :trino-connector:trino-connector-435-436:assemble`，确认子模块通过复用 `trino-connector` 输出即可产出 connector 包。
4. [x] 同样注册 `:trino-connector:trino-connector-437-439`，并配置 437 默认版本的依赖链。
5. [x] 构建 `trino-connector-437-439` 模块，验证共享 `trino-connector` 代码路径可兼容 437–439 版本。

## 工作项 2：创建 `trino-connector-435-439`

1. [ ] 在 settings.gradle.kts 中注册 `:trino-connector:trino-connector-435-439`。
2. [ ] 建立目录与 `build.gradle.kts`：
  - [ ] 应用 `java-library`（可选 `java-test-fixtures`）；
  - [ ] `implementation(project(":trino-connector:trino-connector-common"))`；
  - [ ] `compileOnly` 引入 Trino 435–439 段 `trino-spi`、`trino-plugin`，保持 JDK 17 toolchain。
3. [ ] 在模块内创建插件入口类和 `META-INF/services` 占位，暂不迁移现有实现，确保结构完整。
4. [ ] 更新 `trino-connector/build.gradle.kts` 让该子模块参与 `assemble`/`publish` 流程，但暂不添加测试。
5. [ ] 记录后续差异处理计划（如 SPI 改动、适配层），当前目标是搭建模块与 Gradle 配置。

