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

# Gravitino Agent Guidelines

## General Coding Standards
- **Language**: Use English for all code, comments, and documentation.
- **Style**: Follow rigid Google Java Style. Run `./gradlew spotlessApply` to format.
- **Imports**: prioritizing standard imports over Fully Qualified Class Names (FQN).
  - **Bad**: `org.apache.gravitino.rel.Table table = ...;`
  - **Good**: `Table table = ...;` (with `import org.apache.gravitino.rel.Table;`)
- **Safety**: Use `@Nullable` annotations. Handle resources with try-with-resources.
- **Logging**: Use SLF4J. No `System.out.println`.
- **Testing**:
  - Write unit tests for ALL new logic. NO tests = NO merge.
  - Use `TestXxx` naming pattern (e.g., `TestCatalogService`).
  - Run tests: `./gradlew test -PskipITs`.
  - Docker tests: Tag with `@Tag("gravitino-docker-test")`.
- **Class Member Ordering**: Follow the order:
  1. `static` constants (e.g., `LOG`).
  2. `static` fields.
  3. Instance fields.
  4. Constructors.
  5. Methods (Group by visibility, putting `private` methods at the end).

## Project Structure
- `api/`: Public interfaces.
- `common/`: Shared utilities/DTOs.
- `core/`: Main server logic.
- `server/`: REST API implementation.
- `catalogs/`: Catalog implementations (Hive, Iceberg, MySQL, etc.).
- `clients/`: Java/Python clients.

## Build Commands
- **Build**: `./gradlew build -PskipDockerTests=false`
- **Format**: `./gradlew spotlessApply`
- **Unit Tests**: `./gradlew test -PskipITs -PskipDockerTests=false`
- **Integration Tests**: `./gradlew test -PskipTests -PskipDockerTests=false`
