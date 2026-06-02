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
- **Javadoc**: All new `public` and `protected` classes, methods, and fields must have Javadoc. Missing Javadoc fails the checkstyle CI step (runs with `-Werror`).
- **Imports**: Always use normal `import` statements instead of Fully Qualified Class Names (FQN) in Java code whenever possible.
  - **Bad**: `org.apache.gravitino.rel.Table table = ...;`
  - **Good**: `Table table = ...;` (with `import org.apache.gravitino.rel.Table;`)
  - Do not write inline types like `java.nio.file.Paths` or `org.apache.xxx.Table` unless there is a real class name conflict that cannot be resolved cleanly.
  - If two classes share the same simple name, prefer imports plus small refactors over keeping FQNs throughout the code.
- **Safety**: Use `@Nullable` annotations. Handle resources with try-with-resources.
- **Logging**: Use SLF4J. No `System.out.println`.
- **Testing**:
  - Write unit tests for ALL new logic. NO tests = NO merge.
  - Use `TestXxx` naming pattern (e.g., `TestCatalogService`).
  - Run tests: `./gradlew test -PskipITs`.
  - Docker tests: Tag with `@Tag("gravitino-docker-test")`; run them with `-PskipDockerTests=false`.
- **Class Member Ordering**: Follow the order:
  1. `static` constants (e.g., `LOG`).
  2. `static` fields.
  3. Instance fields.
  4. Constructors.
  5. Methods (Group by visibility, putting `private` methods at the end).

## Code Design & Structure
- **Method granularity**: Each method must earn its existence. Avoid small, fragmented single-use helpers — keep cohesive logic together. At the same time, fold genuinely duplicated logic into one shared helper instead of repeating it. Extract when it is reused or names a genuinely non-obvious step; inline trivial one-liners.
- **No dead parameters or methods**: Remove a parameter that is always the same constant (for example, a flag that is always `true`) and a method that does nothing meaningful. Keep related declarations and their setup together (for example, a thread-pool field and the loop that submits tasks to it).
- **Field assignment**: Qualify field assignments with `this.` (for example, `this.workers = ...`) in constructors and setters.
- **Test-only code must not live in production classes**: Never add a production method or field that exists only to serve a test.
  - Prefer keeping test-only logic in the test module (test helpers, fixtures, or the test class itself).
  - If a production member genuinely must be reachable from a test, annotate it with `@VisibleForTesting` and keep the narrowest visibility possible (package-private, not `public`/`protected`). Never widen visibility solely for a test.
  - Do not reach into non-public members from tests via reflection (`setAccessible(true)`). Add a `@VisibleForTesting` constructor/factory instead.
- **Narrowest visibility**: Give every class member the smallest visibility that works (`private` first, then package-private). Do not make something `public`/`protected` unless an external caller needs it.
- **Dependency injection, not singletons**: Avoid the singleton pattern (including lazy-holder/double-checked instances) — it is hard to test. Construct collaborators once in the owning class (for example, the plugin/bootstrap class) and inject them; do not `new` a manager/service inline in the middle of logic, and never create two instances of a component that is meant to be shared.
- **Naming follows existing conventions**: Match the naming patterns already used by sibling classes and the surrounding package, and make the name reflect the class's actual role. Established suffixes include `XxxManager`, `XxxDispatcher`, `XxxService`, `XxxOperations`, `XxxListener`, `XxxPoller`, capability interfaces `SupportsXxx`, and tests `TestXxx`. Capitalize acronyms consistently with existing code (`RESTUtils`, not `RestUtils`). Name boolean fields without an `is` prefix (`basicAuthEnabled`, not `isBasicAuthEnabled`). Do not invent a new convention when a matching one already exists.
- **Reuse existing types and patterns**: Before adding a new request DTO, PO, or helper, check whether an existing one (for example, an `XxxAssociateRequest` or `XxxPO`) already fits, and reuse it. Validate request DTOs at the request boundary (reject nulls, overlapping add/remove sets, etc.).
- **Exceptions**: Throw the standard, semantically correct exception type for the situation (for example, the project's unauthorized/forbidden exception for auth failures). Do not add a `catch (RuntimeException e) { throw e; }` that adds no behavior, and collapse identical sibling `catch` blocks. When refactoring, preserve identifying context (such as the table identifier) in error messages and logs.
- **Prefer standard-library and Commons idioms**: Use `StringUtils.isBlank`/`isNotBlank` instead of manual null/empty string checks, a `Set` (not a `List`) for repeated membership lookups, and `Base64.getEncoder().encodeToString(...)` rather than manual byte/string round-trips. Do not use deprecated APIs (for example, `RandomStringUtils.randomAlphabetic`); keep a `@SuppressWarnings("deprecation")` only when it is required for compatibility, with a comment explaining why.
- **Component placement & lifecycle**: Put a class in the module that owns its concern. A sub-component of another component (for example, a side module of the entity store) should be created, started, and closed by its owner — mirroring sibling components such as `RelationalGarbageCollector` — rather than being exposed and wired as a global `GravitinoEnv` component.
- **Prefer capability interfaces over concrete casts**: Detect optional behavior through a `SupportsXxx` capability interface plus `instanceof`, not by casting to a concrete implementation class.
- **Argument validation**: Use `Preconditions.checkArgument(condition, message)` for argument/state validation; do not use `checkNotNull`. Only null-check values that can actually be null — do not add defensive null checks for values guaranteed non-null by their source (for example, rows returned by a MyBatis mapper).
- **Remove superseded code**: When you replace an implementation, delete the old classes, methods, fields, and imports it leaves behind. Do not leave orphaned or unreferenced code.

## Configuration Changes
- When adding a new `ConfigEntry`, document it in the matching table in `docs/gravitino-server-config.md` (config name, description, default value, required, and since version).
- Any test that initializes the owning component with a **mocked `Config`** must stub the new key. A real `Config` returns the entry's registered default, but a Mockito-mocked `Config` returns `null` and will throw `NullPointerException` on unboxing (for example, to `long`). Follow how existing keys such as `STORE_DELETE_AFTER_TIME` are already stubbed.
- Validate config values at the entry (for example, `checkValue(v -> v > 0, ...)`). Once validated, do not re-guard the value downstream (no `Math.max(1, ...)` on an already-positive value).

## Concurrency & Background Tasks
- **Lifecycle flags**: A `running`/`started` flag that can be touched concurrently must be an `AtomicBoolean` guarded with `compareAndSet`. Make `start()` idempotent, and make it fail fast (not run with shut-down executors) if called after `close()`.
- **Shared mutable state**: Guard shared mutable state and watch for TOCTOU windows when one thread updates a shared map/token that another reads. When the same collection is handed to multiple consumers, pass an immutable view (for example, `Collections.unmodifiableList(...)`).
- **Scheduled tasks must self-protect**: A `Runnable` scheduled on a `ScheduledExecutorService` must catch and log its own exceptions — an uncaught throwable silently cancels all future executions. Advance cursors/last-run timestamps in a `finally` block so a transient failure does not cause tight-loop retries, and restore the interrupt flag on `InterruptedException`.
- **Thread pools**: Create executors with an explicit bounded queue (`new ThreadPoolExecutor(..., new ArrayBlockingQueue<>(n), ...)`); avoid `Executors.newFixedThreadPool`, whose queue is unbounded. Give pool threads meaningful daemon names. On shutdown, call `awaitTermination(...)` instead of a fire-and-forget `shutdownNow()`.

## API, Errors & Performance
- **Absent values**: Return `Optional<T>` rather than `null` for "absent". Do not use exceptions to signal a normal "not found" outcome — it is misleading and can defeat caching (a cache loader that throws will not memoize the negative result).
- **Don't discard meaningful return values**: If a method returns a success/failure signal (for example, a CAS-style update), check it and at least log a warning on failure so the outcome is observable.
- **Minimize IO round-trips**: On hot/critical paths, do not issue two DB or IO calls where one suffices (for example, a select followed by an update that could be a single statement). Combine queries and avoid redundant reads.
- **Bounded memory**: Do not materialize potentially unbounded data (for example, every reachable file path of a large table) into an in-memory collection. Stream or process it incrementally.
- **No test-scoped libraries in production code**: Libraries such as Awaitility are `testImplementation`-scoped; do not call them from `main` code. Provide a production wait/poll helper instead.
- **Explain magic numbers**: Give a non-obvious numeric constant a short explanatory comment or a named constant.

## Documentation
- **User guides describe usage, not internals**: Keep `docs/` how-to guides focused on how to use a feature; leave implementation and design rationale to the design docs. Do not forward-reference components (for example, a "garbage collector") that the guide never explains.
- **Consistent terminology**: Use one capitalization/spelling for a term throughout (for example, `IdP`, not a mix of `IdP`/`IDP`), matching the surrounding docs.
- **Runnable examples**: Shell snippets must work as written — for example, use `base64 -w 0` (or pipe through `tr -d '\n'`) so an `Authorization` header has no embedded newline.

## Create Issue and PR Guidelines
[IMPORTANT] Before creating an issue or PR using the gh command or the GitHub MCP server, please show a preview of the PR/issue first. Submit it only after I confirm. The issue/PR format should follow the reference and keep the content concise and clear.
- **Issue Templates**: Use the appropriate template from `.github/ISSUE_TEMPLATE/`
- **PR Description**: Follow the template in `.github/PULL_REQUEST_TEMPLATE`
- **Keep PRs focused**: A PR should contain only changes for its stated goal. Do not bundle unrelated refactors, formatting churn, or CI fixes; if you spot an unrelated issue, file/handle it separately.

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
- **OpenAPI Docs Validation**: `./gradlew :docs:build` — Run this after any changes to `docs/open-api/*.yaml` to validate OpenAPI specification correctness.

## Claude Memory Usage
- Before starting any task, use mcp-search to check if similar work has been done before.
  When encountering unfamiliar code or configuration, search memory for prior context.
- When hitting a problem, search memory first for known solutions before debugging from scratch.
- After completing a task, save key findings and solutions to claude-mem for future reference.
- Use multiple keyword combinations when searching (e.g., module name + issue type, class name + error).
