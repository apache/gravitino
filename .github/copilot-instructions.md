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

# Copilot Instructions for Gravitino

## Critical Rules
- **Java Imports**: ALWAYS use standard imports. NEVER use Fully Qualified Class Names (FQN) inside methods unless absolutely necessary for collision resolution.
  - BAD: `org.apache.gravitino.NameIdentifier id = ...`
  - GOOD: `NameIdentifier id = ...` (add `import org.apache.gravitino.NameIdentifier;`)
- **Coding Style**: Strict adherence to Google Java Style.
- **Testing**:
  - EVERY change must have a corresponding test.
  - Do NOT modify existing tests unless the logic has fundamentally changed.
  - Use `TestXxx` prefix for test classes (e.g., `TestMetalake`).
- **Dependencies**: Do NOT add new dependencies without explicit user request.
- **Documentation**: JavaDoc is required for all public APIs.

## Language Specifics
- **Java**:
  - Use `@Nullable` for optional fields/params.
  - Avoid `System.out.println`. Use SLF4J logging.
  - Use `Preconditions.checkArgument` for validation.
  - **Class Member Ordering**: Follow the order:
    1. `static` constants (e.g., `LOG`).
    2. `static` fields.
    3. Instance fields.
    4. Constructors.
    5. Methods (Group by visibility, putting `private` methods at the end).
- **Python**:
  - Follow PEP 8.
  - Type hints are mandatory.

## General
- **Brevity**: Write concise, efficient code. Avoid boilerplate where possible.
- **Safety**: Always check for nulls and handle exceptions specifically (no generic `catch (Exception e)`).

## Review Checklist (Keep It Short)
- **License/Legal**: New files must include Apache License 2.0 header; dependency/license changes must update LICENSE/NOTICE as required.
- **Compatibility**: Avoid breaking changes unless explicitly justified and documented (including migrations when needed).
- **Java hygiene**: No wildcard imports; close resources (try-with-resources); do not leave TODO/FIXME without an issue reference.
- **Security**: No hardcoded secrets; validate external inputs; prevent injection/path traversal/SSRF where applicable; do not log sensitive data.
- **APIs**: Public APIs require JavaDoc; REST APIs must enforce authentication/authorization and return correct HTTP status codes with consistent error payloads.
- **Testing**: Cover error paths and edge cases; keep tests isolated/non-flaky; Docker-dependent tests must be tagged with `@Tag("gravitino-docker-test")`.
