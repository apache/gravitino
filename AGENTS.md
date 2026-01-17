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

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Gravitino is a high-performance, geo-distributed, and federated metadata lake. It manages metadata directly in different sources, types, and regions, providing users with unified metadata access for data and AI assets.

Gravitino acts as a centralized metadata management layer that:
- Provides unified metadata access across diverse data sources (Hive, MySQL, Iceberg, Kafka, etc.)
- Enables end-to-end data governance with access control, auditing, and discovery
- Supports geo-distributed architectures for multi-region and multi-cloud deployments
- Integrates seamlessly with query engines like Trino and Spark
- Manages both data assets and AI model metadata

## Project Requirements

- Always use English in code, documentation, examples, and comments
- Follow Apache Software Foundation guidelines and best practices
- Code should be clean, maintainable, efficient, and well-documented
- All public APIs must have comprehensive documentation
- Maintain backward compatibility - breaking changes require careful consideration
- Write meaningful tests for all features and bug fixes - **code without tests will not be merged**
- Follow the project's coding standards and use Spotless for formatting

## Architecture

The project is organized as a Gradle multi-module project with the following key components:

### Core Modules
- `api/` - Public API definitions and interfaces for Gravitino
- `common/` - Common utilities, types, and shared code across modules
- `core/` - Core Gravitino server implementation and metadata management logic
- `server/` - REST API server implementation and HTTP endpoints
- `server-common/` - Shared server utilities and configurations

### Client Modules
- `clients/client-java/` - Java client library for Gravitino API
- `clients/client-python/` - Python client library with bindings
- `clients/cli/` - Command-line interface for Gravitino
- `clients/filesystem-hadoop3/` - Hadoop FileSystem implementation
- `clients/filesystem-fuse/` - FUSE filesystem integration (optional)

### Catalog Implementations
- `catalogs/catalog-common/` - Base classes and utilities for catalog implementations
- `catalogs/catalog-hive/` - Apache Hive metastore catalog
- `catalogs/catalog-lakehouse-iceberg/` - Apache Iceberg catalog
- `catalogs/catalog-lakehouse-paimon/` - Apache Paimon catalog
- `catalogs/catalog-lakehouse-hudi/` - Apache Hudi catalog
- `catalogs/catalog-lakehouse-generic/` - Generic lakehouse catalog
- `catalogs/catalog-jdbc-common/` - Base JDBC catalog functionality
- `catalogs/catalog-jdbc-mysql/` - MySQL catalog
- `catalogs/catalog-jdbc-postgresql/` - PostgreSQL catalog
- `catalogs/catalog-jdbc-doris/` - Apache Doris catalog
- `catalogs/catalog-jdbc-oceanbase/` - OceanBase catalog
- `catalogs/catalog-jdbc-starrocks/` - StarRocks catalog
- `catalogs/catalog-kafka/` - Apache Kafka catalog
- `catalogs/catalog-fileset/` - Fileset catalog for file-based data
- `catalogs/catalog-model/` - AI model catalog

### Integration Connectors
- `spark-connector/` - Spark connector for Gravitino (Spark 3.3, 3.4, 3.5)
- `flink-connector/` - Apache Flink connector (Scala 2.12 only)
- `trino-connector/` - Trino connector for federated queries

### Standalone Services
- `iceberg/iceberg-rest-server/` - Standalone Iceberg REST catalog service
- `lance/lance-rest-server/` - Lance format REST server
- `mcp-server/` - Model Context Protocol server for integrating Gravitino with AI assistants

### Authorization
- `authorizations/authorization-common/` - Authorization framework
- `authorizations/authorization-ranger/` - Apache Ranger integration
- `authorizations/authorization-chain/` - Chain-based authorization

### Other Components
- `web/` - Web UI frontend (Next.js/React)
- `docs/` - Documentation source files
- `integration-test-common/` - Common utilities for integration tests (includes MiniGravitino)
- `bundles/` - Cloud storage bundles (AWS, GCP, Azure, Aliyun)
- `lineage/` - Data lineage tracking
- `maintenance/` - Maintenance jobs and optimizer modules
- `mcp-server/` - Model Context Protocol server for AI assistant integration

## Common Development Commands

### Build System

Gravitino uses Gradle as its build system. All commands should be run from the project root.

#### Basic Build Commands

```bash
# Clean build without tests (fast)
./gradlew clean build -x test

# Full build with all tests
./gradlew build -PskipDockerTests=false

# Build specific module
./gradlew :module-name:build

# Compile and package distribution
./gradlew compileDistribution

# Create distribution tarball
./gradlew assembleDistribution

# Build with all deprecation warnings (useful for development)
./gradlew build --warning-mode all
```

#### Scala Version Selection

Gravitino supports Scala 2.12 and 2.13 (default is 2.12):

```bash
# Build with Scala 2.12 (default)
./gradlew build -PscalaVersion=2.12

# Build with Scala 2.13
./gradlew build -PscalaVersion=2.13
```

#### Python Client Build

The default Python version is 3.9, but you should match your local Python version. For example, with Python 3.11:

```bash
# Build with specific Python version (matches local environment)
./gradlew build -PpythonVersion=3.11

# Or modify gradle.properties to set: pythonVersion=3.11
```

Available versions: 3.9, 3.10, 3.11, 3.12

#### Connector Builds

```bash
# Build Spark connector for Spark 3.4 with Scala 2.12
./gradlew spark-connector:spark-runtime-3.4:build -PscalaVersion=2.12

# Build Trino connector
./gradlew assembleTrinoConnector

# Build Iceberg REST server
./gradlew assembleIcebergRESTServer
```

#### Web UI Development

```bash
# Install dependencies and run web UI in development mode
cd web
npm install
npm run dev

# Build web UI for production
npm run build

# Run web UI tests
npm run test
```

### Code Quality and Formatting

Gravitino uses Spotless for code formatting:

```bash
# Check code formatting
./gradlew spotlessCheck

# Apply code formatting (ALWAYS run before committing)
./gradlew spotlessApply

# Compile triggers spotless check
./gradlew compileJava
```

### Testing

#### Unit Tests

```bash
# Run all unit tests (skip integration tests)
./gradlew test -PskipITs

# Run tests for specific module
./gradlew :module-name:test

# Run specific test class
./gradlew :module-name:test --tests "com.example.TestClass"

# Run specific test method
./gradlew :module-name:test --tests "com.example.TestClass.testMethod"
```

#### Integration Tests

Integration tests can run in two modes: `embedded` (default) and `deploy`.

**Embedded mode** uses `MiniGravitino` from `integration-test-common/`:
- Starts Gravitino in-process for faster testing
- No need to build distribution
- Easier debugging (tests run in same JVM)
- Ideal for rapid development and unit testing

**Deploy mode** uses full distribution:
- More realistic testing environment
- Requires `./gradlew compileDistribution` first
- Tests Gravitino as it would run in production
- Slower but catches deployment-specific issues

```bash
# Run integration tests in embedded mode (uses MiniGravitino)
./gradlew test -PskipTests -PtestMode=embedded

# Run integration tests in deploy mode (requires distribution)
./gradlew compileDistribution
./gradlew test -PskipTests -PtestMode=deploy

# Enable Docker-based tests
./gradlew test -PskipDockerTests=false
```

#### Docker Test Environment

For macOS users running Docker tests:

```bash
# Option 1: Use OrbStack (recommended)
# Install from https://orbstack.dev/

# Option 2: Use mac-docker-connector
./dev/docker/tools/mac-docker-connector.sh
```

### Running Gravitino Server

```bash
# Start server (after building)
./bin/gravitino.sh start

# Stop server
./bin/gravitino.sh stop

# Or from distribution
./distribution/package/bin/gravitino.sh start
```

## Key Technical Details

### Language and JDK Requirements

- **Build JDK**: Java 17 is **required** - the build will fail with other versions
- **Runtime JDK**: Java 17 (for server and connectors)
- **Target Compatibility**: Client-side modules (clients, connectors) target JDK 8 for compatibility
- **Scala**: Supports 2.12 (default) and 2.13 (Flink only supports 2.12)
- **Python**: 3.9, 3.10, 3.11, or 3.12

### Build System Details

- Gradle 8.x with Kotlin DSL
- Gradle Java Toolchain for automatic JDK management
- Error Prone for additional compile-time checks
- JaCoCo for code coverage reporting

### Testing Framework

- JUnit 5 (JUnit Platform) for Java tests, Python unittest for Python tests
- Testcontainers for Docker-based integration tests
- `@Tag("gravitino-docker-test")` marks tests requiring Docker
- Separate test modes: embedded (MiniGravitino) and deploy (full distribution)

### Code Organization Patterns

- **API-first design**: Public APIs defined in `api/` module
- **Catalog pattern**: All catalog implementations extend `BaseCatalog` from `core/` module
- **Connector pattern**: Query engine connectors provide transparent Gravitino integration
- **Configuration**: Uses `.conf` files and environment variables
- **REST API**: JAX-RS based RESTful services
- **Event listeners**: `core/listener/` module contains event listener interfaces for catalog and metalake changes
- **Transaction management**: Uses `javax.transaction` for distributed transaction support

### Metadata Model Hierarchy

Gravitino organizes metadata in a hierarchical structure:

1. **Metalake** - Top-level isolation boundary for multi-tenancy
2. **Catalog** - Connection to a metadata source (Hive, Iceberg, MySQL, Kafka, etc.)
3. **Schema/Namespace** - Logical grouping of objects (database in Hive, schema in relational DBs, topic namespace in Kafka)
4. **Table/Topic/Fileset/Model** - Actual metadata objects depending on catalog type
5. **Column/Partition** - Fine-grained metadata for tables

Each level in the hierarchy supports:
- Properties metadata for configuration validation
- Event listeners for change notification
- Authorization checks (if enabled)
- Audit logging

### Core Architectural Components

- **Entity system**: Core entity management (`Entity`, `EntityStore`) handles all metadata objects
- **CatalogOperations**: Interface that catalogs implement for metadata operations (schemas, tables, etc.)
- **Properties metadata**: Each catalog has `PropertiesMetadata` defining valid configuration properties
- **Hook system**: `core/hook/` supports pre/post operation hooks for extensibility
- **Cache layer**: `core/cache/` provides caching for catalog metadata
- **Credential system**: `core/credential/` manages secure credential storage and encryption
- **Authorization**: `authorizations/` modules provide access control (Ranger, chain-based)
- **Audit**: `core/audit/` provides audit logging for all operations

### Database Backend Support

Gravitino supports multiple backend databases for metadata storage:
- H2 (default for testing)
- MySQL
- PostgreSQL
- Configure via `jdbcBackend` property

### REST API Configuration

- **Default port**: 8090
- **Base path**: `/api/`
- **Authentication**: Supports OAuth2, Kerberos, and simple auth
- **API documentation**: YAML specs in `docs/open-api/`

### Docker Images

The project uses custom CI Docker images:
- `apache/gravitino-ci:hive-{version}` - Hive testing
- `apache/gravitino-ci:doris-{version}` - Doris testing
- `apache/gravitino-ci:trino-{version}` - Trino testing
- `apache/gravitino-ci:kerberos-hive-{version}` - Kerberos-enabled Hive
- `apache/gravitino-ci:ranger-{version}` - Apache Ranger

## Development Tips

### Code Standards

- **Minimize dependencies**: Avoid adding unnecessary dependencies
- **Prefer Apache-licensed libraries**: Ensure license compatibility
- **Handle resources properly**: Always close resources; use try-with-resources
- **Null safety**: Use `@Nullable` annotations and proper null checks
- **Immutability**: Prefer immutable objects where possible
- **Thread safety**: Document thread-safety guarantees in public APIs

### Testing Best Practices

- **Test coverage**: All new features must include unit tests
- **Integration tests**: Mark Docker-dependent tests with `@Tag("gravitino-docker-test")`
- **Test data**: Use `test/resources` for static test data
- **Cleanup**: Ensure tests clean up resources (temp files, Docker containers)
- **Isolation**: Tests should not depend on execution order
- **Assertions**: Use meaningful assertion messages
- **Naming**: Use the `TestXxx` prefix for Java test classes (for example, `TestCatalogService`), not the `XxxTest` suffix

### Documentation Standards

- **Javadoc**: Required for all public APIs
- **Pythondoc**: Use docstrings for Python public APIs
- **OpenAPI**: Document REST APIs using YAML syntax in `docs/open-api/`
- **Markdown**: Documentation in `docs/` directory
- **Comments**: Explain "why" not "what" in code comments
- **Changelog**: Document user-facing changes

### Common Pitfalls to Avoid

- **Don't commit without running Spotless**: Always run `./gradlew spotlessApply`
- **Don't skip tests in PRs**: CI will run them anyway
- **Don't break backward compatibility**: Discuss API changes on dev@ mailing list
- **Don't add dependencies without review**: Consider licensing and maintenance burden
- **Don't ignore Docker test failures**: Ensure Docker environment is properly set up
- **Don't mix concerns**: Keep API, implementation, and tests separate

### Debugging

#### Debug Integration Tests (Embedded Mode)

Set breakpoints directly in IntelliJ IDEA or your IDE - tests run in the same process.

#### Debug Integration Tests (Deploy Mode)

1. Enable debug mode in `distribution/package/conf/gravitino-env.sh`:
   ```bash
   GRAVITINO_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
   ```

2. Start Gravitino server manually:
   ```bash
   ./distribution/package/bin/gravitino.sh start
   ```

3. Attach remote debugger to port 5005 in your IDE

#### Useful Gradle Tasks

```bash
# Show all tasks
./gradlew tasks

# Show task dependencies
./gradlew :module-name:taskName --dry-run

# Show dependency tree
./gradlew :module-name:dependencies

# Show all dependencies (including transitive)
./gradlew :module-name:allDeps

# Check JVM toolchain configuration
./gradlew javaToolchains
```

## Project Structure Best Practices

### Adding a New Catalog

1. Create module under `catalogs/catalog-{name}/`
2. Extend `BaseCatalog` and implement required interfaces
3. Add configuration classes
4. Implement operations (tables, schemas, etc.)
5. Add comprehensive tests
6. Update `settings.gradle.kts` to include module
7. Add documentation in `docs/`

### Adding a New Connector

1. Create module under `{engine}-connector/`
2. Implement engine-specific integration
3. Follow engine's plugin/connector architecture
4. Add integration tests
5. Document installation and configuration
6. Update build to create distribution package

### Module Dependencies

- API modules should have minimal dependencies
- Client modules should not depend on server-related modules
- Keep catalog implementations independent
- Use `catalog-common` for shared catalog code
- Avoid circular dependencies

## Code Review Guidelines

For comprehensive code review guidelines, see `.github/copilot-instructions.md`. Key areas include:

### Code Quality
- Follows project coding standards
- Properly formatted with Spotless
- No compiler warnings (build uses `-Werror`)
- Error Prone checks pass
- Appropriate use of logging

### Testing
- Unit tests for all new functionality
- Integration tests for end-to-end scenarios
- Tests are properly tagged (e.g., `@Tag("gravitino-docker-test")`)
- Test names clearly describe what is being tested
- Assertions have meaningful messages

### Documentation
- Public APIs have complete Javadoc and Pythondoc
- REST APIs documented with YAML syntax in the `docs/open-api/` directory
- Complex logic has explanatory comments
- User-facing changes documented in `docs/`
- README or docs updated if needed

### API Design
- Backward compatible (or breaking change is justified and documented)
- Consistent with existing APIs
- Proper use of interfaces and abstract classes
- Appropriate exception handling
- Thread-safety considerations documented

### License Compliance
- All new files have Apache license header
- Third-party dependencies are Apache-compatible
- No GPL or other incompatible licenses
- LICENSE and NOTICE files updated if needed

## Useful Links

- **Main Website**: https://gravitino.apache.org
- **Documentation**: https://gravitino.apache.org/docs/latest/
- **GitHub Repository**: https://github.com/apache/gravitino
- **Issue Tracker**: https://github.com/apache/gravitino/issues
- **Mailing List**: dev@gravitino.apache.org
- **ASF Guidelines**: https://www.apache.org/dev/

## Quick Reference

### Essential Files
- `build.gradle.kts` - Root build configuration
- `settings.gradle.kts` - Module definitions
- `gradle.properties` - Build properties and flags
- `CONTRIBUTING.md` - Contribution guidelines
- `README.md` - Project overview
- `docs/` - Complete documentation

### Key Gradle Properties
- `scalaVersion` - Scala version (2.12 or 2.13)
- `pythonVersion` - Python version (3.9, 3.10, 3.11, 3.12)
- `skipTests` - Skip unit tests
- `skipITs` - Skip integration tests
- `skipDockerTests` - Skip Docker-based tests
- `testMode` - Test mode (embedded or deploy)
- `jdbcBackend` - Database backend (h2, mysql, postgresql)

### Important Environment Variables
- `JAVA_HOME` - Java installation directory
- `GRAVITINO_HOME` - Gravitino installation directory (deploy mode)
- `GRAVITINO_TEST` - Enable test mode
- `HADOOP_USER_NAME` - Hadoop user for tests

Remember: Gravitino follows Apache Software Foundation practices. Be respectful, collaborative, and follow the community guidelines in all interactions.
