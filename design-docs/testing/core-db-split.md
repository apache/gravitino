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

# SPIP: Split core tests into explicit database lanes

| | |
|---|---|
| **Status** | Implemented on PR [apache/gravitino#13517](https://github.com/apache/gravitino/pull/13517); this document records the final design retroactively |
| **Scope** | `core` module test execution, `core/build.gradle.kts`, `dev/ci/core_test_identity.py`, `.github/workflows/build.yml` |
| **Format** | Apache Spark SPIP (Heilmeier Catechism) |

## Q1. What are you trying to do?

Make it explicit, per test class, which database(s) a core test runs against, and make the
build honor that declaration exactly.

Concretely:

- Replace the single `:core:test` run with four Gradle lanes: `coreUnitTest` (no database, no
  Docker), `coreH2Test`, `coreMySQLTest`, and `corePostgreSQLTest`.
- Give contributors one typed way to put a class into a lane: `@CoreBackend.H2`,
  `@CoreBackend.MySQL`, `@CoreBackend.PostgreSQL`, or `@CoreBackend.All`. No raw tag strings.
- Guarantee that a class not tagged for a lane leaves no trace in that lane's output, so CI can
  prove the three database lanes ran the same test contract.
- Give a developer a way to ask "which lane does my class run in?" without running any test.
- Warn a developer off the old, unsplit `:core:test` entry point without deleting it.

## Q2. What problem is this proposal NOT designed to solve?

These are deliberate exclusions, not oversights.

- **No CI guard for the orphan case.** A class that carries `gravitino-docker-test` but no
  `@CoreBackend.*` annotation is excluded from `coreUnitTest` (by the Docker tag) and from every
  backend lane (no backend tag), so it runs nowhere. The design does not add a CI step that
  scans for this. Mitigation is developer self-service: `./gradlew :core:coreTestLaneOf`
  prints an explicit warning for exactly this shape, and the build script comment on
  `coreBackendTestTags` documents it.
- **No cross-lane report aggregation.** Each lane writes its own JUnit XML, HTML report, and
  JaCoCo `.exec` under a lane-specific path. CI uploads them as four separate evidence artifacts
  and only the JaCoCo data is merged (for coverage). There is no tool that merges the four JUnit
  reports into one; readers open the lane they care about.
- **Not removing `:core:test`.** It is the `java` plugin's built-in `test` task; other tooling
  (IDEs, scripts) may still target it by convention, so it stays registered and functional rather
  than being deleted. It is deprecated *in place* instead (see Q4/Appendix C): running it directly
  now prints a warning naming the four real lanes, but it still runs and still passes.
- **Not changing how a test selects its backend at runtime.** `BackendTestSelector` (reads the
  `gravitino.core.test.backend` system property), `BackendTestExtension` (`@TestTemplate`
  invocation contexts), and the `storageProvider()` parameter pattern are unchanged. The lane
  decides *whether* a class runs; these decide *what it does* once it runs.

## Q3. How is it done today, and what are the limits of current practice?

Before this PR, `core` ran every test through one `:core:test` task. H2-backed, MySQL-backed,
and PostgreSQL-backed tests were all discovered by the same task, and the only filtering was
the repository-wide `excludeTags("gravitino-docker-test")` applied when Docker was unavailable.
There was no notion of a backend lane at all, so there was nothing for CI to reconcile.

The first iteration on this PR branch introduced four lane tasks but decided membership with a
gatekeeper tag plus modifier tags:

```kotlin
// state at 654cfe33a, since replaced
useJUnitPlatform {
  if (backend == null) {
    excludeTags(coreDatabaseTestTag, "gravitino-docker-test")
  } else {
    includeTags(coreDatabaseTestTag)                        // gatekeeper
    when (backend) {
      "h2"         -> excludeTags(coreMySQLTestTag, corePostgreSQLTestTag)
      "mysql"      -> excludeTags(coreH2TestTag, corePostgreSQLTestTag)
      "postgresql" -> excludeTags(coreH2TestTag, coreMySQLTestTag)
    }
  }
}
```

A class had to carry `gravitino-core-database-test` to enter any database lane, and then
optionally carried per-backend tags whose *absence* meant "all backends". This had three
concrete failure modes, all silent:

1. **Gatekeeper desync.** A class with correct per-backend tags but no gatekeeper tag was
   invisible to every lane. Nothing failed; the tests just never ran.
2. **Two tags meant zero lanes.** Because each lane *excluded* the other two backends' tags, a
   class tagged for both H2 and MySQL was excluded from H2 (carries MySQL tag), from MySQL
   (carries H2 tag), and from PostgreSQL. An intermediate fix (46eef2130) replaced the
   `when` with a boolean tag expression to make multi-tagged classes run under each lane, but
   the expression was hard to read and still depended on the gatekeeper.
3. **Typos compiled.** Tags were raw `@Tag("gravitino-core-h2-test")` string literals. A
   misspelling was a valid, unrelated tag, so the class silently dropped out of its lane.

There was also no way to answer "where will this class run?" other than running a lane and
grepping the resulting XML.

## Q4. What is new in your approach, and why do you think it will be successful?

### Three peer tags, nothing else

The gatekeeper is gone. Lane membership is decided by exactly three JUnit tags, defined once
in `core/build.gradle.kts` and mirrored as constants in `CoreBackend`:

```kotlin
val coreBackendTestTags = linkedMapOf(
  "h2"         to "gravitino-core-h2-test",
  "mysql"      to "gravitino-core-mysql-test",
  "postgresql" to "gravitino-core-postgresql-test"
)
```

Each backend lane is a plain `includeTags(ownBackendTag)`. The unit lane is
`excludeTags(<all three>, "gravitino-docker-test")`. "Runs on all backends" means carrying all
three tags explicitly, never carrying none. A class tagged for two backends runs in both, because
each lane only looks for its own tag and never excludes another backend's.

### Typed annotations that are pure `@Tag` composition

Contributors never write tag strings. `CoreBackend` is a final namespace class with four nested
marker annotations; each is meta-annotated with `@Tag` (one or three) and the standard
`@Documented @Inherited @Retention(RUNTIME) @Target(TYPE)` set, and nothing else. This is the
single most important property of the design: **filtering happens at JUnit discovery time**.
Gradle's `includeTags`/`excludeTags` become a JUnit Platform `PostDiscoveryFilter`, which reads
class tags reflectively via `AnnotationSupport.findRepeatableAnnotations(clazz, Tag.class)`.
Because the annotations carry no `@ExtendWith` or other execution-time hook, an excluded class is
pruned from the discovered test plan before execution begins and therefore produces no
`<testcase>` element, not even a `<skipped/>` one, in the lane's JUnit XML.

That hard-zero property is what `dev/ci/core_test_identity.py reconcile` depends on (Appendix
B). An earlier redesign on this same branch (`@DatabaseTest` + a `BackendLaneCondition`
`ExecutionCondition`, commit efee6a4c9) was reverted (547e9bdf0) precisely because an
`ExecutionCondition` runs after discovery: the disabled class still appeared in every lane's XML
as `<skipped/>`, the manifest step saw foreign backend markers, and reconcile failed.

`TestCoreDatabaseLaneAnnotations` pins this contract as a regression guard: it asserts the exact
tag expansion of each annotation, that stacking two annotations yields both tags, that subclasses
inherit the tags, and that none of the four annotation types carries any meta-annotation outside
`{Documented, Inherited, Retention, Target, Tag, Tags}`. Re-introducing an execution-time hook
fails that test.

### A local, discovery-only lane check

`./gradlew :core:coreTestLaneOf -PclassName=<FQCN>` runs `CoreTestLaneOf`, which loads the class,
calls the same `AnnotationSupport.findRepeatableAnnotations` lookup the engine uses, maps the
resulting tag set to lane task names, and prints them. If the class carries
`gravitino-docker-test` but no backend tag, it prints an explicit "will NOT run in ANY lane"
warning. Because it re-derives the answer through the identical mechanism, its output is
trustworthy without running a single test.

### `:core:test` deprecated in place, not removed

`:core:test` is the `java` plugin's built-in `test` task, auto-registered before this module's
build script runs. Removing it outright risks breaking any tooling (IDE run buttons, scripts)
that targets `:<module>:test` by convention across the whole repo, not just `core`. Instead its
`tasks.test { ... }` configuration block gained a `doFirst` that logs a warning naming the four
lanes and `coreTestLaneOf` every time it runs, so it still works exactly as before but visibly
tells a developer it is the wrong task. No CI change was needed: `dev/ci/test-shards.sh` already
emits `-x :core:test` for the `others` shard, so the warning only ever fires for someone running
it directly. See Appendix C for the day-to-day commands this replaces.

### Alternatives considered (brief)

- **Parameterized annotation** (`@CoreDBTest(backends = {H2, MYSQL})`): mechanically possible
  with a custom `PostDiscoveryFilter` that reads the attribute, but rejected. JUnit's built-in
  tag resolution only reads an annotation type's fixed meta-annotations, never a per-usage
  attribute, so it would need a bespoke filter to interpret. More importantly,
  `core_test_identity.py reconcile` requires a fixed, small set of lanes with exact identity
  equality across the three database lanes; a free-form attribute-driven subset per class would
  undermine that invariant rather than express it.
- **Flat top-level annotation names** (`@CoreH2Test`, `@CoreMySQLTest`, ...): rejected after a
  real, reproduced compile error. `TestJdbcPartitionStatisticStorageIT` already declares nested
  classes named `H2Test`, `MySQLTest`, and `PostgreSQLTest`; an unqualified annotation import of
  the same simple name was shadowed by the nested class, producing
  `H2Test cannot be converted to Annotation`. Namespacing under `CoreBackend` makes the
  reference always qualified, so the collision is structurally impossible, and it matches the
  package's existing "Backend" vocabulary (`BackendTestExtension`, `BackendTestSelector`).
- **Execution-time condition** (`@DatabaseTest` + `ExecutionCondition`): implemented, reverted;
  see above.

### Why it will work

The design has already been run, not just reasoned about. Real `:core:coreH2Test`,
`:core:coreUnitTest`, and `:core:coreTestLaneOf` invocations on the branch confirmed that only
the matching nested class's XML is produced for `TestJdbcPartitionStatisticStorageIT`, that an
`@CoreBackend.All` class is excluded from `coreUnitTest` ("No tests found for given includes")
and included in `coreH2Test`, that the manifest step passes on the lane output, and that
`coreTestLaneOf` reports lanes for a real class and warns for a synthetic orphan.

## Q5. Who cares? If you are successful, what difference will it make?

- **Contributors adding core storage tests** get a two-line, compile-checked way to declare
  where a test runs, and a five-second local command to confirm it. The "my test never ran and
  nobody noticed" class of bug is reduced to one remaining shape (the orphan), which the local
  tool names explicitly.
- **Reviewers** can read lane membership off the class declaration instead of reconstructing it
  from a tag expression in Gradle.
- **CI maintainers** get a lane filter that is three trivial include/exclude lines, a
  reconcile step whose hard-zero precondition is guaranteed by construction, and a regression
  test that fails if anyone reintroduces an execution-time hook.
- **The project** keeps the ability to prove, on every PR, that H2, MySQL, and PostgreSQL ran
  the identical normalized test contract, and that unit and database identities are disjoint.

## Q6. What are the risks?

- **Orphan classes run nowhere and nothing in CI says so.** A `gravitino-docker-test` class with
  no `@CoreBackend.*` annotation is dropped by every lane. Accepted by design (Q2); mitigated by
  `coreTestLaneOf`'s explicit warning and by documentation, not enforcement.
- **Tag string drift.** `CoreBackend.H2_TAG` / `MYSQL_TAG` / `POSTGRESQL_TAG` must stay equal to
  the values in `coreBackendTestTags`. They are declared in two places (Kotlin build script and
  Java test source) with no shared source. Mitigated by a comment on each side naming the other;
  a mismatch surfaces as an empty lane ("No tests found for given includes") rather than silent
  success, because the lane would then include a tag no class carries.
- **`:core:test` still works and still means "everything".** It is deprecated in place, not
  removed (Q4), so a developer can still run the unsplit task locally and get results that do not
  correspond to any CI lane; the `doFirst` warning is advisory, not a hard failure, so it is easy
  to miss in a noisy log.
- **Sharded reports.** With no aggregation tooling, someone looking for "all core test
  results" must open up to four reports. Accepted as scope simplification.
- **Partial-backend classes are only CI-legal as normalized siblings.** A class annotated with a
  single backend (or two) passes reconcile only if matching classes exist for the backends it
  omits and `core_test_identity.py`'s normalization maps them to the same identity (today: the
  `TestJdbcPartitionStatisticStorageIT$H2Test/$MySQLTest/$PostgreSQLTest` shape, handled by
  `STATS_BACKEND_CLASS_RE`). A new single-backend class without siblings fails reconcile with
  "Database identity mismatch". This is the intended contract, but it is a constraint contributors
  must know; it is documented in `CoreBackend`'s Javadoc.

## Q7. How long will it take?

**Done, on the PR branch:**

- `CoreBackend` annotation namespace (`H2`, `MySQL`, `PostgreSQL`, `All`).
- Four Gradle lane tasks via `registerCoreTestTask`, with the three-peer-tag filter.
- `coreTestLaneOf` JavaExec task and `CoreTestLaneOf` main class.
- `TestCoreDatabaseLaneAnnotations` regression guard.
- Migration of all three existing usage patterns (`AbstractEntityStorageTest`,
  `TestJDBCBackend`, `TestJdbcPartitionStatisticStorageIT`) and removal of the gatekeeper tag.
- Removal of the dead `compare-legacy` subcommand from `core_test_identity.py`.
- CI wiring: `build` matrix shards `core-unit`/`core-h2`/`core-mysql`/`core-postgresql` each run
  one lane and upload evidence; `core-test-contract` (`needs: [changes, build]`) runs the tool's
  own unit tests, downloads the four evidence artifacts, and reconciles.
- `:core:test` deprecated in place: a `doFirst` warning on its `tasks.test` block names the four
  lanes and `coreTestLaneOf`; the task still runs and still passes.

Contributor-facing documentation for the lanes lives in this document (Appendix C) rather than in
`docs/how-to-test.md`, which is a repo-wide doc unrelated to this split (it only documents the
root `./gradlew test` task and does not mention `core` or its lanes) - no edit there was needed.

Found and fixed during review, before merge: `TestCoreDatabaseLaneAnnotations`'s
`@ParameterizedTest` used the default display name, which embeds each case's expected-tags
argument (e.g. `[gravitino-core-h2-test]`) into its own JUnit XML - since that test carries no
`@CoreBackend.*` annotation itself, it runs in `coreUnitTest`, and `manifest`'s unit-lane check
rejects any standalone backend token there as a foreign marker. Reproduced directly
(`./gradlew :core:coreUnitTest --tests ...TestCoreDatabaseLaneAnnotations` then `manifest --lane
unit` failed with "contains an explicit backend marker ['h2']"), fixed by naming on the class
under test only (`@ParameterizedTest(name = "{index}: {0}")`), re-verified clean, and confirmed
by scanning all 1968 `coreUnitTest` testcases through the real `normalize_identity` function with
zero marker errors.

**Optional follow-up, not required to ship:** `STATS_BACKEND_CLASS_RE` (B.2/B.3) is hard-coded to
one outer class name, so it does not generalize to a second nested-per-backend class without
editing the regex. Appendix C now tells contributors to prefer `@CoreBackend.All` and flags this
constraint explicitly rather than silently teaching a pattern that fails `reconcile`; generalizing
the regex to any outer class name is a real improvement but touches CI-wired parsing logic and
needs its own fixtures/tests, so it was left out of this pass.

**Remaining, in scope, not yet done:**

- Reply to the open review thread on #13517 and update the PR description to match the final
  design.

## Q8. What are the mid-term and final "exams" to check for success?

Each criterion is concrete and checkable.

**Mid-term (already verifiable on the branch):**

1. `./gradlew :core:coreUnitTest -PskipITs --tests '*TestCoreDatabaseLaneAnnotations*'` passes: each
   annotation expands to exactly its expected tags, stacking and inheritance hold, and no
   annotation carries a non-tag meta-annotation.
2. `./gradlew :core:coreH2Test` produces JUnit XML under `core/build/test-results/coreH2Test/`
   containing no `<testcase>` whose classname or name carries a `mysql` or `postgresql` marker
   (`python3 dev/ci/core_test_identity.py manifest --lane h2 ...` exits 0).
3. `./gradlew :core:coreUnitTest` on an `@CoreBackend.All` class reports "No tests found for
   given includes" for that class.
4. `./gradlew :core:coreTestLaneOf -PclassName=org.apache.gravitino.storage.relational.TestJDBCBackend`
   prints `runs in: coreH2Test, coreMySQLTest, corePostgreSQLTest` in under five seconds once
   test classes are compiled, without executing any test.
5. `coreTestLaneOf` on a class tagged only `gravitino-docker-test` prints the "will NOT run in
   ANY lane" warning.
6. `./gradlew :core:test -PskipITs --tests <any class>` still passes and now also logs the
   `:core:test is deprecated ...` warning naming the four lanes and `coreTestLaneOf`.

**Final (CI, on every PR touching core):**

7. All four `build` shards succeed and each uploads `core-<lane>-test-evidence` containing a
   non-empty `<lane>.json` manifest, JUnit XML, HTML report, and JaCoCo `.exec`.
8. `core-test-contract` passes: `reconcile` reports `database_identities_equal: true` and
   `unit_database_disjoint: true`, i.e. the H2, MySQL, and PostgreSQL manifests hold identical
   normalized identity multisets and the unit manifest shares none of them.
9. No `@Tag("gravitino-core-*-test")` string literal exists in `core/src/test` (all lane
   membership goes through `@CoreBackend.*`).

---

## Appendix A: API Changes

### A.1 `CoreBackend` annotations

Location: `core/src/test/java/org/apache/gravitino/storage/relational/CoreBackend.java`.
Test-source only; not part of any published artifact.

```java
public final class CoreBackend {
  public static final String H2_TAG         = "gravitino-core-h2-test";
  public static final String MYSQL_TAG      = "gravitino-core-mysql-test";
  public static final String POSTGRESQL_TAG = "gravitino-core-postgresql-test";

  private CoreBackend() {}

  @Documented @Inherited @Retention(RUNTIME) @Target(TYPE)
  @Tag(H2_TAG)
  public @interface H2 {}

  @Documented @Inherited @Retention(RUNTIME) @Target(TYPE)
  @Tag(MYSQL_TAG)
  public @interface MySQL {}

  @Documented @Inherited @Retention(RUNTIME) @Target(TYPE)
  @Tag(POSTGRESQL_TAG)
  public @interface PostgreSQL {}

  @Documented @Inherited @Retention(RUNTIME) @Target(TYPE)
  @Tag(H2_TAG) @Tag(MYSQL_TAG) @Tag(POSTGRESQL_TAG)
  public @interface All {}
}
```

Semantics:

| Declaration | Tags carried | Lanes |
|---|---|---|
| (none) | none | `coreUnitTest` |
| `@CoreBackend.H2` | `h2` | `coreH2Test` |
| `@CoreBackend.H2 @CoreBackend.MySQL` | `h2`, `mysql` | `coreH2Test`, `coreMySQLTest` |
| `@CoreBackend.All` | `h2`, `mysql`, `postgresql` | all three backend lanes |
| `@Tag("gravitino-docker-test")` only | `docker` | **none** (orphan) |

`@Target(TYPE)` restricts the annotations to classes. `@Inherited` means an abstract base class
can carry the annotation and every concrete subclass (including Jupiter `@Nested` classes and
`TestJDBCBackend` subclasses) inherits lane membership. The three `*_TAG` constants are the
Java-side mirror of `coreBackendTestTags` in `core/build.gradle.kts` and must be kept equal.

Usage patterns as migrated on the PR:

```java
// Multi-backend via a parameter provider; the lane's system property narrows storageProvider().
@CoreBackend.All
abstract class AbstractEntityStorageTest {
  static Object[][] storageProvider() { /* filtered by BackendTestSelector.isSelected */ }
}

// Multi-backend via @TestTemplate; BackendTestExtension emits one invocation for the lane's backend.
@CoreBackend.All
@ExtendWith({BackendTestExtension.class, ...})
public abstract class TestJDBCBackend { ... }

// One @Nested class per single backend; each nested class is its own lane member.
@Tag("gravitino-docker-test")
public class TestJdbcPartitionStatisticStorageIT {
  @Nested @CoreBackend.MySQL      @Tag("gravitino-docker-test") static class MySQLTest      extends Base {}
  @Nested @CoreBackend.PostgreSQL @Tag("gravitino-docker-test") static class PostgreSQLTest extends Base {}
  @Nested @CoreBackend.H2                                       static class H2Test         extends Base {}
}
```

### A.2 Gradle tasks

Location: `core/build.gradle.kts`.

```kotlin
val coreBackendTestTags = linkedMapOf(
  "h2" to "gravitino-core-h2-test",
  "mysql" to "gravitino-core-mysql-test",
  "postgresql" to "gravitino-core-postgresql-test"
)
val coreTestBackendProperty = "gravitino.core.test.backend"

fun registerCoreTestTask(taskName: String, backend: String? = null) =
  tasks.register<Test>(taskName) { ... }

registerCoreTestTask("coreUnitTest")
registerCoreTestTask("coreH2Test", "h2")
registerCoreTestTask("coreMySQLTest", "mysql")
registerCoreTestTask("corePostgreSQLTest", "postgresql")

tasks.register<JavaExec>("coreTestLaneOf") { ... }
```

Per-task configuration set by `registerCoreTestTask`:

| Property | Unit lane (`backend == null`) | Backend lane |
|---|---|---|
| `useJUnitPlatform` filter | `excludeTags(h2, mysql, postgresql, "gravitino-docker-test")` | `includeTags(coreBackendTestTags[backend])` |
| `systemProperty(gravitino.core.test.backend)` | not set | `backend` |
| `extraProperties["includeDockerTaggedTests"]` | not set (root build applies its default) | `true` (root build does not add `excludeTags("gravitino-docker-test")`) |
| `maxParallelForks` / `junit.jupiter.execution.parallel.enabled` | default | `1` / `false` |
| Docker precondition (`doFirst`) | none | for `mysql`/`postgresql`: fail unless `rootProject.extra["dockerTest"] == true` |
| JUnit XML | `core/build/test-results/<task>/` | same |
| HTML report | `build/reports/tests/core/<task>/` | same |
| JaCoCo exec | `core/build/jacoco/<task>.exec` | same |
| Up-to-date inputs | `coreTestSuite=unit`, `coreTestBackend=none`, `coreTestIncludesDockerTaggedTests=false` | `coreTestSuite=<backend>`, `coreTestBackend=<backend>`, `coreTestIncludesDockerTaggedTests=true` |

Adding a backend is one map entry plus one `registerCoreTestTask(...)` call plus one nested
annotation in `CoreBackend`; the filter code needs no change.

The lane JaCoCo files feed `validateCoreSuiteCoverage` and `jacocoTestReport` when
`-PcoreSuiteCoverage=true`, which is how the CI `coverage` job merges the four lanes.

### A.3 `coreTestLaneOf` task and `CoreTestLaneOf` tool

Gradle side:

```kotlin
tasks.register<JavaExec>("coreTestLaneOf") {
  group = "verification"
  dependsOn(tasks.named("testClasses"))
  classpath = sourceSets["test"].runtimeClasspath
  mainClass.set("org.apache.gravitino.storage.relational.CoreTestLaneOf")
  doFirst {
    val className = project.findProperty("className") as? String
      ?: throw GradleException("Usage: ./gradlew :core:coreTestLaneOf -PclassName=<fully.qualified.ClassName>")
    args(className)
  }
}
```

Java side, `core/src/test/java/org/apache/gravitino/storage/relational/CoreTestLaneOf.java`:

```java
public final class CoreTestLaneOf {
  public static void main(String[] args)  // exactly one arg: a fully qualified class name
}
```

Behaviour:

- `args.length != 1` or class not on the test classpath: message to stderr, exit code 1.
- Otherwise prints `<FQCN> tags: [...]` followed by exactly one of:
  - `<FQCN> runs in: coreH2Test, coreMySQLTest, corePostgreSQLTest` (subset, in that order),
  - `<FQCN> carries gravitino-docker-test but no backend tag - it will NOT run in ANY lane. Add ...`,
  - `<FQCN> carries no backend tag - runs in coreUnitTest.`
- Exit code 0 in all three printed cases; the orphan case is a warning, not a failure, so it can
  be used interactively without special-casing.

It runs only `testClasses` (compilation), never a `Test` task.

### A.4 `dev/ci/core_test_identity.py`

Two subcommands remain (the unwired `compare-legacy` subcommand was removed on this PR):

```
core_test_identity.py manifest  --lane {unit,h2,mysql,postgresql} --results <dir> --output <json>
core_test_identity.py reconcile --manifests <unit.json> <h2.json> <mysql.json> <postgresql.json> --output <json>
```

Unit tests: `dev/ci/tests/test_core_test_identity.py`, run by the `core-test-contract` job
before reconcile.

---

## Appendix B: Design Sketch

### B.1 Discovery-time versus execution-time filtering

JUnit Platform runs a test task in two phases. **Discovery** builds a `TestPlan`: the Jupiter
engine scans the class directories, creates a `ClassTestDescriptor` per test class, and attaches
each descriptor's tags. `PostDiscoveryFilter`s then prune descriptors from that plan. **Execution**
walks the surviving plan, evaluates `ExecutionCondition`s, and runs (or skips) each node.

Gradle's `useJUnitPlatform { includeTags(...) / excludeTags(...) }` is compiled into a
`TagFilter`, which is a `PostDiscoveryFilter`. A descriptor excluded by it is removed from the plan
before execution starts. Gradle's XML reporter only sees the executed plan, so an excluded class
contributes no `<testsuite>` and no `<testcase>` elements at all.

An `ExecutionCondition` (what the reverted `BackendLaneCondition` was) runs at execution time. A
disabled class is still in the plan; Jupiter reports it as skipped, and Gradle writes a
`<testcase ...><skipped/></testcase>` for each of its methods. That is a *trace*, and the
reconcile invariant in B.3 tolerates no trace.

The shipped design therefore uses tags only. The `@CoreBackend.*` annotations exist solely so that
contributors do not type tag strings; at the JUnit level they are indistinguishable from writing
`@Tag("gravitino-core-h2-test")` directly.

### B.2 How JUnit resolves the tags

Jupiter collects a class's tags with
`AnnotationSupport.findRepeatableAnnotations(clazz, Tag.class)`. That lookup:

1. Reads the class's directly present annotations.
2. Follows `@Inherited` annotations up the superclass chain.
3. For each annotation found, recursively inspects the annotation *type's* own meta-annotations,
   so a `@CoreBackend.All` on the class yields the three `@Tag` meta-annotations declared on
   `CoreBackend.All`.
4. Unwraps the `@Tags` container so repeated `@Tag`s are returned individually.

Two consequences shape the API:

- Tags come from an annotation type's fixed meta-annotations, never from an attribute value on
  the usage site. This is why a parameterized `backends = {...}` attribute cannot participate in
  standard tag filtering and would need a custom filter.
- Only annotations reachable through this reflective walk count. Any annotation whose meaning
  depends on code running (an `@ExtendWith` extension, a condition) is invisible to the tag
  filter. `TestCoreDatabaseLaneAnnotations.testNoneOfThemCarryAnExecutionTimeHook` enforces that
  the four annotation types declare nothing outside `{Documented, Inherited, Retention, Target,
  Tag, Tags}`, so the annotations cannot acquire execution-time behaviour without breaking the
  test.

`CoreTestLaneOf` calls exactly this `findRepeatableAnnotations` method and then applies the same
membership rule the Gradle filter applies (`tags ∩ {h2, mysql, postgresql}`), which is why its
answer is authoritative without running a lane. `TestCoreDatabaseLaneAnnotations.tagsOf` uses the
same call, so the regression guard, the local tool, and the engine share one lookup.

### B.3 How `core_test_identity.py` validates lane membership

**`manifest --lane L --results DIR`** parses every `TEST-*.xml` under `DIR` and, for each
`<testcase classname=C name=N>`:

1. Extracts backend markers from `C` (via `STATS_BACKEND_CLASS_RE`, matching
   `TestJdbcPartitionStatisticStorageIT$<backend>Test`) and from `N` (via
   `TEST_TEMPLATE_BACKEND_RE`, matching `[<BACKEND> Backend]` as emitted by
   `BackendTestExtension`, and `BACKEND_TOKEN_RE`, matching a standalone `h2`/`mysql`/`postgresql`
   token as emitted by parameterized `storageProvider()` names).
2. Fails closed (`ManifestError`, exit 1) if `L == unit` and any marker is present, or if `L` is
   a database lane and any marker other than `L` is present ("foreign backend marker").
3. Normalizes the identity: nested stats class names collapse to `...$BackendTest`, backend
   tokens collapse to `BACKEND`, `[BACKEND Backend]`, and trailing `[n]` invocation indices
   collapse to `[INDEX]`.
4. Counts the normalized `(classname, name)` pair in a multiset, counts status
   (`passed`/`skipped`/`failures`/`errors`), and sums `time`.

It also fails closed on missing XML, zero testcases, any failure or error, and malformed
durations. The output JSON carries the identity list, counts, and a SHA-256 `identity_digest`.

Note step 2 does not look at status: a `<skipped/>` testcase is still a testcase. This is the
precise reason execution-time skipping is incompatible with the design. Under the reverted
`ExecutionCondition` scheme, `TestJdbcPartitionStatisticStorageIT$MySQLTest` appeared in the H2
lane's XML as skipped, step 2 saw a `mysql` marker in the `h2` lane, and the manifest step
failed. Even a class with no recognizable markers would have broken reconcile, since a skipped
entry present in one lane but absent from another changes the multiset.

**`reconcile --manifests unit h2 mysql postgresql`** loads exactly four manifests (one per lane,
no duplicates, no missing), re-validates each (schema, counts, digest, no failures), then asserts:

- `counters[mysql] == counters[h2]` and `counters[postgresql] == counters[h2]` as multisets,
  reporting up to five missing/extra identities on mismatch.
- `counters[unit] & counters[h2]` is empty (unit and database identities are disjoint).

It writes `summary.json` with `database_identities_equal`, `unit_database_disjoint`, per-lane
summaries, and combined counts. Any violation exits 1 and fails the `core-test-contract` job.

### B.4 End-to-end flow in CI

```
changes ──► build (matrix: core-unit | core-h2 | core-mysql | core-postgresql | ...)
              │  each core-* shard:
              │    ./gradlew :core:<laneTask> ...            (tag filter prunes at discovery)
              │    core_test_identity.py manifest --lane L   (fails on foreign markers)
              │    validate evidence files exist and are non-empty
              │    upload core-<L>-test-evidence
              ▼
           core-test-contract  (needs: [changes, build])
              │    unittest dev/ci/tests/test_core_test_identity.py
              │    download the four evidence artifacts
              │    core_test_identity.py reconcile --manifests unit h2 mysql postgresql
              │    upload summary.json as core-test-contract
              ▼
           coverage  (needs core-test-contract; merges the four JaCoCo .exec files)
```

`dev/ci/test-shards.sh` maps `build/core-<lane>` to `:core:<laneTask>` and emits `-x :core:test`
for the `others` shard so the unsplit task never runs in CI.

### B.5 Invariants, in one place

1. A class is in lane `L` iff its `findRepeatableAnnotations(Tag)` set contains `L`'s tag
   (backend lanes) or contains none of the three backend tags and not `gravitino-docker-test`
   (unit lane).
2. Every `@CoreBackend.*` type declares only `Tag`/`Tags` plus the four standard
   meta-annotations. (Guarded by `TestCoreDatabaseLaneAnnotations`.)
3. A lane's JUnit XML contains testcases only for classes in that lane. (Follows from 1 and 2
   via discovery-time pruning; checked by `manifest`'s foreign-marker rule.)
4. The h2, mysql, and postgresql lanes yield identical normalized identity multisets, and the
   unit lane is disjoint from them. (Checked by `reconcile`.)
5. `CoreBackend.*_TAG == coreBackendTestTags[*]`. (Not machine-checked; documented on both
   sides.)

---

## Appendix C: How to run core tests locally

This is the contributor-facing walkthrough; `docs/how-to-test.md` covers the repo-wide
`./gradlew test` task and does not mention `core` specifically, so it was left unchanged and this
appendix is the source of truth for the lanes instead.

### Running a lane

```bash
./gradlew :core:coreUnitTest                              # no database, no Docker - the default
./gradlew :core:coreH2Test                                 # H2-backed tests, no Docker
./gradlew :core:coreMySQLTest -PskipDockerTests=false       # MySQL-backed, needs Docker
./gradlew :core:corePostgreSQLTest -PskipDockerTests=false  # PostgreSQL-backed, needs Docker
```

`coreMySQLTest`/`corePostgreSQLTest` fail fast with a clear `GradleException` if Docker isn't
running and `-PskipDockerTests=false` wasn't passed - they won't silently no-op. `coreH2Test` needs
neither Docker nor that flag. Each lane runs its `Test` task sequentially
(`maxParallelForks = 1`) because database tests mutate process-wide state.

Do **not** run `./gradlew :core:test` - it is deprecated in place (Q4/A.2): it still works, but
warns and does not correspond to any of the four lanes above or any CI shard.

### Tagging a new test class

Pick the annotation that matches where the class needs to run, from
`org.apache.gravitino.storage.relational.CoreBackend`:

```java
@CoreBackend.All                         // against all three backends - the default choice
public abstract class MyMultiBackendTest { ... }

@CoreBackend.H2                          // only against H2 - a genuinely H2-only test
public class MyH2OnlyTest { ... }
```

No annotation at all means the class is a plain unit test and runs only in `coreUnitTest`.

**Use `@CoreBackend.All` unless the class is genuinely single-backend.** Stacking a subset (e.g.
`@CoreBackend.H2 @CoreBackend.MySQL`) compiles and each lane it names runs the class, but CI's
`reconcile` step then requires a *normalized sibling* in every backend lane it omits (Appendix
A.1/A.4, `CoreBackend`'s Javadoc) - today the only shape that satisfies that is one `@Nested`
class per backend under a shared outer class, following
`TestJdbcPartitionStatisticStorageIT`'s `H2Test`/`MySQLTest`/`PostgreSQLTest` pattern exactly
(`core_test_identity.py`'s normalization is hard-coded to that one outer class name - see B.3). A
standalone partial-backend class without that sibling structure passes locally in the lanes it
runs in and then fails `reconcile` in CI with "Database identity mismatch". If in doubt, use
`@CoreBackend.All`. See A.1's usage-pattern table for the three concrete shapes already in the
codebase (parameter-provider, `@TestTemplate`, and one `@Nested` class per backend).

### Checking where a class lands, before running anything

```bash
./gradlew :core:coreTestLaneOf -PclassName=org.apache.gravitino.storage.relational.TestJDBCBackend
```

Compiles test sources (nothing else) and prints the class's tags and the lane(s) it runs in, or an
explicit warning if it carries `gravitino-docker-test` with no `@CoreBackend.*` annotation - the
one case that silently drops a class out of every lane (Q6). Run this after adding or changing an
annotation on a database-touching test class, before pushing.
