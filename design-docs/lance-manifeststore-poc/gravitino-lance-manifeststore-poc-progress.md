# Gravitino Lance ManifestStore POC 进展记录

## 2026-05-12

### Task 0: 准备阶段

状态：已完成

进展：

1. 完成 Lance 方案五 POC 需求文档。
2. 完成 Lance 方案五 POC 设计文档。
3. 完成后续 agent 开发任务书。
4. 确认本地工作区当前还没有 Gravitino 源码，需要拉取仓库并自建开发环境。

下一步：

1. 检查本机 `git`、Java、Maven/Gradle 可用性。
2. 拉取 Gravitino 仓库到本地。
3. 在源码中完成代码定位和 code map。

### Task 1: 本地仓库定位与代码摸底

状态：已完成

进展：

1. 确认 Gravitino 仓库位于 `D:\ai\project\gravitino`，当前分支为 `main`。
2. 确认 Lance REST 入口位于 `lance/lance-rest-server`。
3. 确认表操作主链路为：
   `LanceTableOperations -> LanceCatalog -> GravitinoLanceTableOperations`。
4. 确认当前已存在 namespace/table 相关 route，但没有 TableVersion route。
5. 确认现有测试入口包括 REST 资源测试和集成测试：
   `TestGravitinoLanceTableOperations`、`LanceRESTServiceIT`。
6. 产出 code map 文档，后续按该路径实现。

下一步：

1. 继续确认 Lance Namespace 依赖模型里是否已有 `CreateTableVersion` 等请求对象。
2. 确认 relational metadata store 的新增表、mapper 和事务接入方式。
3. 开始实现最小 `CreateTableVersion` 路由和服务骨架。

### Task 2: Linux 开发环境决策

状态：已完成

进展：

1. 确认 Gravitino 根构建要求 JDK 17。
2. 已在 Windows 主机安装 JDK 17。
3. 确认当前仓库只有 Unix 风格 `gradlew`，Windows 侧构建入口不顺滑。
4. 确认当前 `wsl.exe` 可用，但本机还没有安装任何 Linux 发行版。
5. 决定后续开发切换到 WSL Linux 环境，以降低 Gradle、脚本、路径和集成测试兼容成本。

下一步：

1. 安装 Ubuntu WSL 发行版。
2. 在 WSL 中初始化 JDK 17、Git、Gradle/Wrapper 所需环境。
3. 在 Linux 环境下继续 Gravitino 的编译、开发和测试验证。

### Task 3: Ubuntu WSL 导入

状态：已完成

进展：

1. 已下载官方 Ubuntu 24.04 WSL 镜像到：
   `D:\ai\project\wsl\images\ubuntu-24.04.4-wsl-amd64.wsl`
2. 发现当前开发工具运行在隔离用户 `CodexSandboxOffline` 下，不是桌面登录用户。
3. 当前用户上下文直接 `wsl --import` 会触发 `E_ACCESSDENIED`。
4. 通过提权上下文确认 WSL 已可见两个发行版：
   `Ubuntu`、`Ubuntu-24.04`
5. 已完成 `Ubuntu-24.04` 首次启动，能正常进入 root shell。
6. 已在 WSL 中安装开发依赖：
   `openjdk-17-jdk`、`git`、`curl`、`unzip`、`zip`、`build-essential`
7. 已在 WSL 中安装并验证 `Gradle 8.2`：
   `/opt/gradle/gradle-8.2/bin/gradle --version`
8. 已修复仓库中 `gradlew` 的 CRLF 行尾问题，避免 WSL 中出现 `/bin/sh^M` 类错误。

下一步：

1. 在 WSL 内解析 Lance 依赖模型，确认 `CreateTableVersion` 等对象是否已有现成类型。
2. 继续实现 Gravitino Lance TableVersion route、service、metadata store。
3. 在 WSL 内执行编译、单测和集成测试验证。

### Task 4: TableVersion POC first implementation batch

Status: in progress
Progress:
1. Added Lance table version relational persistence in `core`:
   `LanceTableVersionPO`
   `LanceTableVersionMapper`
   `LanceTableVersionSQLProviderFactory`
   `LanceTableVersionBaseSQLProvider`
2. Added `lance_table_version_info` schema entries for H2 / MySQL / PostgreSQL.
3. Added Lance TableVersion POC DTOs in `lance-common/model`.
4. Added `GravitinoLanceTableVersionRegistry` with:
   `createTableVersion`
   `listTableVersions`
   `describeTableVersion`
   `batchCreateTableVersions`
   `batchDeleteTableVersions`
5. Extended `LanceTableOperations` and `GravitinoLanceTableOperations` to expose the new APIs.
6. Added REST resource:
   `lance-rest-server/.../LanceTableVersionOperations.java`
7. Added REST resource tests and integration-test skeletons for version registration paths.

Current blockers:
1. WSL Gradle build remains unstable:
   - some runs fail in plugin dependency TLS handshake
   - some runs proceed further but are interrupted by existing repo-wide spotless issues
2. Need a stable compile/test entry to confirm any real Java compile errors in the new code.

Next:
1. Stabilize compile path and capture true module compile result.
2. Fix any Java compile issues revealed by the build.
3. Run REST resource tests.
4. Run at least one end-to-end integration test for create/list/describe and batch rollback.
