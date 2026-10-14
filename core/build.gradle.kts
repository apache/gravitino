import com.sun.management.OperatingSystemMXBean
import net.ltgt.gradle.errorprone.errorprone
import org.apache.gravitino.testing.CoreDatabaseConcurrency
import org.apache.gravitino.testing.CoreDatabaseConcurrency.DetectedCapacity
import org.apache.gravitino.testing.DbConnectionInfo
import org.apache.gravitino.testing.SharedDbContainerService
import org.gradle.api.provider.Provider
import java.lang.management.ManagementFactory

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.jcstress)
  alias(libs.plugins.jmh)
  alias(libs.plugins.aspectj.post.compile.weaving)
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":catalogs:catalog-common"))
  implementation(libs.aspectj.aspectjrt)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.metrics)
  implementation(libs.bundles.prometheus)
  implementation(libs.caffeine)
  implementation(libs.commons.dbcp2)
  implementation(libs.commons.io)
  implementation(libs.commons.lang3)
  implementation(libs.commons.collections4)
  implementation(libs.concurrent.trees)
  implementation(libs.guava)
  implementation(libs.h2db)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.dataformat.yaml)
  implementation(libs.jackson.jaxrs.json.provider) // This is required by lance
  implementation(libs.lance) {
    exclude(group = "com.fasterxml.jackson.core", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.datatype", module = "*") // provided by gravitino
    exclude(group = "commons-codec", module = "commons-codec") // provided by jcasbin
    exclude(group = "com.google.guava", module = "guava") // provided by gravitino
    exclude(group = "org.apache.commons", module = "commons-lang3") // provided by gravitino
    exclude(group = "org.junit.jupiter", module = "*") // provided by test scope
    exclude(group = "com.fasterxml.jackson.jaxrs", module = "jackson-jaxrs-json-provider") // using gravitino's version
    exclude(group = "org.apache.httpcomponents.client5", module = "*") // provided by gravitino
    exclude(group = "org.lance", module = "lance-namespace-core") // This is unnecessary in the core module
    // Same rationale as lance-namespace-core: lance-core 6.0.0 declares
    // lance-namespace-apache-client as a transitive, but core never calls into it.
    // Leaving it on the main classpath shadows the lance-rest aux service's own
    // lance-namespace-apache-client (loaded via lance-rest-server/libs/), and
    // because the aux classloader is parent-first, the older transitive wins
    // on request deserialization (e.g. dropping fields like `check_declared`).
    exclude(group = "org.lance", module = "lance-namespace-apache-client")
  }
  implementation(libs.mybatis)

  annotationProcessor(libs.lombok)

  compileOnly(libs.lombok)
  compileOnly(libs.servlet) // fix error-prone compile error

  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server-common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(libs.awaitility)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)

  testRuntimeOnly(libs.junit.jupiter.engine)

  jcstressImplementation(libs.mockito.core)
  jcstressImplementation(libs.aspectj.aspectjrt)
}

val testJar by tasks.registering(Jar::class) {
  archiveClassifier.set("tests")
  from(sourceSets["test"].output)
}

configurations {
  create("testArtifacts")
}

artifacts {
  add("testArtifacts", testJar)
}

// Core's tests run in three tasks: `test` (unit tests and H2, no Docker), coreMySQLTest and
// corePostgreSQLTest. A class joins a backend through its @CoreBackend.* annotation; see
// CoreBackend.java.
// Backend name -> the JUnit tag that admits a class to that lane. Keep in sync with CoreBackend.
val coreBackendTestTags =
  linkedMapOf(
    "h2" to "gravitino-core-h2-test",
    "mysql" to "gravitino-core-mysql-test",
    "postgresql" to "gravitino-core-postgresql-test"
  )
val coreTestBackendProperty = "gravitino.core.test.backend"
val coreDatabaseTaskNames =
  setOf("coreMySQLTest", "corePostgreSQLTest")
// How many database tasks this build may run at once: those named explicitly, or both when
// `check`/`build` pulls them in with Docker tests on.
val coreDatabaseRequestedTasks = gradle.startParameter.taskNames.map { it.substringAfterLast(':') }
val coreDatabaseActiveLaneCount =
  if (
    rootProject.extra["dockerTest"] == true &&
    coreDatabaseRequestedTasks.any { it == "check" || it == "build" }
  ) {
    coreDatabaseTaskNames.size
  } else {
    coreDatabaseRequestedTasks.distinct().count { it in coreDatabaseTaskNames }.coerceAtLeast(1)
  }
val coreDatabaseTotalMemoryBytes =
  runCatching {
    (ManagementFactory.getOperatingSystemMXBean() as? OperatingSystemMXBean)?.totalMemorySize ?: 0L
  }.getOrDefault(0L)
val coreDatabaseMemoryBasis =
  if (coreDatabaseTotalMemoryBytes > 0) {
    "jdk-total-memory-minus-configured-build-reserve"
  } else {
    "unavailable"
  }
val coreDatabaseMemoryConfiguration =
  CoreDatabaseConcurrency.resolveMemoryConfiguration(
    providers.gradleProperty(CoreDatabaseConcurrency.BUILD_MEMORY_RESERVE_PROPERTY).orNull,
    providers
      .environmentVariable(CoreDatabaseConcurrency.BUILD_MEMORY_RESERVE_ENVIRONMENT_VARIABLE)
      .orNull,
    providers.gradleProperty(CoreDatabaseConcurrency.FORK_MEMORY_OVERHEAD_PROPERTY).orNull,
    providers
      .environmentVariable(CoreDatabaseConcurrency.FORK_MEMORY_OVERHEAD_ENVIRONMENT_VARIABLE)
      .orNull
  )
val coreDatabaseAvailableMemoryBytes =
  CoreDatabaseConcurrency.availableMemoryForTestWorkers(
    coreDatabaseTotalMemoryBytes,
    coreDatabaseMemoryConfiguration.buildReserveBytes()
  )
val coreDatabaseDetectedCapacity =
  DetectedCapacity(
    Runtime.getRuntime().availableProcessors(),
    coreDatabaseTotalMemoryBytes,
    gradle.startParameter.maxWorkerCount,
    coreDatabaseActiveLaneCount
  )
val coreDatabaseConcurrencyBudget =
  CoreDatabaseConcurrency.rolloutBudget(coreDatabaseMemoryConfiguration)
val macDockerConnectorFixedNetwork =
  rootProject.extra[
    CoreDatabaseConcurrency.MAC_DOCKER_CONNECTOR_FIXED_NETWORK_EXTRA
  ] as? Boolean ?: false

// Connection pool budget for the shared-container path: several test-JVM forks may briefly hold
// connections against the same physical server at once, so this stays well under MySQL's default
// max_connections=151 / PostgreSQL's default max_connections=100 even with a handful of forks and
// classes active concurrently, while remaining far larger than any single test actually needs.
val sharedDbTestConnectionPoolSize = 20

// A database the environment injects for a backend, e.g. a service container on a Kubernetes
// runner: GRAVITINO_TEST_MYSQL_URL / _USER / _PASSWORD (or POSTGRESQL). The URL is an admin JDBC
// URL; each test fixture gets its own new database on that server. When set, the task uses it
// and starts no container; otherwise it falls back to Docker.
data class InjectedDatabase(val url: String, val user: String, val password: String)

fun injectedDatabase(backend: String): InjectedDatabase? {
  val prefix = "GRAVITINO_TEST_${backend.uppercase()}_"
  val url = providers.environmentVariable(prefix + "URL").orNull?.takeIf { it.isNotBlank() } ?: return null
  return InjectedDatabase(
    url,
    providers.environmentVariable(prefix + "USER").orNull ?: "root",
    providers.environmentVariable(prefix + "PASSWORD").orNull ?: ""
  )
}

fun registerCoreTestTask(
  taskName: String,
  backend: String
) = tasks.register<Test>(taskName) {
  group = "verification"
  description = "Runs core database tests against $backend."

  testClassesDirs = sourceSets["test"].output.classesDirs
  classpath = sourceSets["test"].runtimeClasspath

  reports.html.outputLocation.set(
    rootProject.layout.buildDirectory.dir("reports/tests/core/$taskName")
  )

  useJUnitPlatform {
    // Plain tag include, applied by JUnit at discovery time, so classes not tagged for this
    // backend never show up in this task's JUnit XML. A class tagged for several backends runs
    // under each of them.
    includeTags(
      coreBackendTestTags[backend] ?: throw GradleException("Unsupported core test backend: $backend")
    )
  }

  val shardTelemetryDirectory =
    layout.buildDirectory.dir("test-results/$taskName/shard-telemetry")
  val laneMaximum = CoreDatabaseConcurrency.laneMaximum(true, macDockerConnectorFixedNetwork)
  val concurrencyResolution =
    CoreDatabaseConcurrency.resolve(
      providers.gradleProperty(CoreDatabaseConcurrency.FORKS_PROPERTY).orNull,
      providers.environmentVariable(CoreDatabaseConcurrency.FORKS_ENVIRONMENT_VARIABLE).orNull,
      coreDatabaseDetectedCapacity,
      coreDatabaseConcurrencyBudget,
      laneMaximum
    )

  systemProperty(coreTestBackendProperty, backend)
  // Read by the root setTestEnvironment inside doFirst, i.e. at execution time, after every
  // configuration action has run - so it doesn't depend on register/configureEach ordering.
  extensions.extraProperties["includeDockerTaggedTests"] = true

  // Each Gradle worker owns its database server. JUnit execution remains serial inside it.
  maxParallelForks = concurrencyResolution.forks()
  systemProperty("junit.jupiter.extensions.autodetection.enabled", "true")
  systemProperty("junit.jupiter.execution.parallel.enabled", "false")
  systemProperty(
    "gravitino.core.database.shard.telemetry.directory",
    shardTelemetryDirectory.get().asFile.absolutePath
  )
  outputs.dir(shardTelemetryDirectory)

  doFirst {
    project.delete(shardTelemetryDirectory)
    logger.lifecycle(
      "[CORE-DB-CONCURRENCY] task={} forks={} source={} limitingFactor={} " +
        "safeMaximum={} allowedMaximum={} processors={} totalMemoryBytes={} " +
        "availableTestMemoryBytes={} memoryBasis={} buildReserveBytes={} " +
        "buildReserveSource={} memoryBytesPerFork={} testWorkerMaxHeapBytes={} " +
        "forkOverheadBytes={} forkOverheadSource={} gradleMaxWorkers={} " +
        "activeDatabaseLanes={} rolloutCap={} laneMaximum={} " +
        "macDockerConnectorFixedNetwork={} junitParallel=false",
      path,
      concurrencyResolution.forks(),
      concurrencyResolution.source(),
      concurrencyResolution.limitingFactor(),
      concurrencyResolution.safeMaximum(),
      concurrencyResolution.allowedMaximum(),
      coreDatabaseDetectedCapacity.processors(),
      coreDatabaseTotalMemoryBytes,
      coreDatabaseAvailableMemoryBytes,
      coreDatabaseMemoryBasis,
      coreDatabaseMemoryConfiguration.buildReserveBytes(),
      coreDatabaseMemoryConfiguration.buildReserveSource(),
      coreDatabaseConcurrencyBudget.memoryBytesPerFork(),
      CoreDatabaseConcurrency.TEST_WORKER_MAX_HEAP_BYTES,
      coreDatabaseMemoryConfiguration.forkOverheadBytes(),
      coreDatabaseMemoryConfiguration.forkOverheadSource(),
      coreDatabaseDetectedCapacity.gradleMaxWorkers(),
      coreDatabaseDetectedCapacity.activeDatabaseLanes(),
      coreDatabaseConcurrencyBudget.rolloutCap(),
      laneMaximum,
      macDockerConnectorFixedNetwork
    )
  }

  doLast {
    val shardRecords =
      shardTelemetryDirectory
        .get()
        .asFile
        .walkTopDown()
        .filter { it.isFile && it.extension == "log" }
        .flatMap { it.readLines().asSequence() }
        .filter { it.startsWith("[CORE-DB-SHARD]") }
        .sorted()
        .toList()
    if (shardRecords.isEmpty()) {
      throw GradleException("The $backend core database lane emitted no shard telemetry.")
    }
    shardRecords.forEach { logger.lifecycle(it) }
  }

  val injected = injectedDatabase(backend)
  doFirst {
    if (injected == null && rootProject.extra["dockerTest"] != true) {
      throw GradleException(
        "$path requires Docker or an injected database; use -PskipDockerTests=false with " +
          "Docker running, or set GRAVITINO_TEST_${backend.uppercase()}_URL."
      )
    }
  }

  // Contract with SharedCoreDatabaseProvisioner (core/src/test/java/.../storage/relational)
  // and its ADMIN_PROPERTY_PREFIX/POOL_MAX_CONNECTIONS_PROPERTY constants - kept as literal
  // strings on both sides rather than importing that class here, since a main build script
  // cannot see a project's own test-source classes at configuration time (they are compiled
  // by a task, not on the script's own classpath).
  val sharedDbAdminPropertyPrefix = "gravitino.test.db."
  systemProperty(sharedDbAdminPropertyPrefix + "pool.maxConnections", sharedDbTestConnectionPoolSize)

  // Must match root build.gradle.kts's own dockerTest wiring exactly (rootProject.extra
  // only, not e.g. System.getenv("dockerTest")): it unconditionally sets this task's
  // dockerTest environment variable from rootProject.extra["dockerTest"], overriding
  // anything a developer exported in their shell, which is what BackendTestExtension
  // actually reads at test-JVM runtime. Gating shared-container startup on anything looser
  // here could start it even when the forked test JVM will never see dockerTest=true and so
  // never use it.
  if (injected != null) {
    // Set at execution time so credentials stay out of the task's cache key.
    doFirst {
      systemProperty(sharedDbAdminPropertyPrefix + backend + ".adminUrl", injected.url)
      systemProperty(sharedDbAdminPropertyPrefix + backend + ".user", injected.user)
      systemProperty(sharedDbAdminPropertyPrefix + backend + ".password", injected.password)
    }
  } else if (rootProject.extra["dockerTest"] == true) {
    @Suppress("UNCHECKED_CAST")
    val sharedDbContainerService =
      rootProject.extra["sharedDbContainerService"] as Provider<SharedDbContainerService>
    usesService(sharedDbContainerService)

    doFirst {
      val connectionInfo: DbConnectionInfo = sharedDbContainerService.get().connectionInfo(backend)
      systemProperty(sharedDbAdminPropertyPrefix + backend + ".adminUrl", connectionInfo.adminJdbcUrl)
      systemProperty(sharedDbAdminPropertyPrefix + backend + ".user", connectionInfo.user)
      systemProperty(sharedDbAdminPropertyPrefix + backend + ".password", connectionInfo.password)
    }
  }
}

registerCoreTestTask("coreMySQLTest", "mysql")
registerCoreTestTask("corePostgreSQLTest", "postgresql")

// `test` runs everything core can run without Docker: unit tests (no backend tag) and classes
// tagged for H2, against H2. Classes tagged only for MySQL or PostgreSQL, and Docker-tagged
// classes with no backend tag (see TestCoreLaneMembership), stay out. One JVM, as before the split.
tasks.test {
  // TestRestrictedRegoGoldenFile reads docs/policies.md.
  inputs.file(rootProject.file("docs/policies.md"))
  systemProperty("gravitino.repositoryRoot", rootProject.projectDir.path)
  systemProperty(coreTestBackendProperty, "h2")
  // Read by the root setTestEnvironment inside doFirst; lets H2 run the Docker-tagged backend
  // classes, which need no container on H2.
  extensions.extraProperties["includeDockerTaggedTests"] = true
  useJUnitPlatform {
    includeTags(
      "${coreBackendTestTags.getValue("h2")} | " +
        "!(${coreBackendTestTags.getValue("mysql")} | " +
        "${coreBackendTestTags.getValue("postgresql")} | gravitino-docker-test)"
    )
  }
  maxParallelForks = 1
  systemProperty("junit.jupiter.execution.parallel.enabled", "false")
}

val coreDatabaseTestTasks = listOf("coreMySQLTest", "corePostgreSQLTest")
// `check` (and so `build`) also runs a database task when Docker tests are on or its database is
// injected. They run no
// tests under -PskipTests, which selects integration tests only and core has none.
tasks.check {
  dependsOn(
    coreDatabaseTestTasks.filter {
      rootProject.extra["dockerTest"] == true ||
        injectedDatabase(it.removePrefix("core").removeSuffix("Test").lowercase()) != null
    }
  )
}
coreDatabaseTestTasks.forEach {
  tasks.named(it) { onlyIf("-PskipTests selects integration tests only") { !rootProject.hasProperty("skipTests") } }
}

// One core report from the three tasks' exec files; missing ones (e.g. no Docker run) are skipped.
tasks.named<JacocoReport>("jacocoTestReport") {
  // `build` with Docker runs the database tasks too; report only after they finish.
  mustRunAfter(coreDatabaseTestTasks)
  executionData.setFrom(
    (listOf("test") + coreDatabaseTestTasks).map { layout.buildDirectory.file("jacoco/$it.exec") }
  )
}

tasks.withType<JavaCompile>().configureEach {
  if (name.contains("jcstress", ignoreCase = true)) {
    options.errorprone.excludedPaths.set(".*/generated/.*")
  }
}

tasks.named<JavaCompile>("jmhCompileGeneratedClasses").configure {
  options.errorprone.isEnabled = false
  options.compilerArgs.removeAll { it.contains("Xplugin:ErrorProne") }
}

jcstress {
  /*
   Available modes:
   - sanity : takes seconds
   - quick : takes tens of seconds
   - default : takes minutes, good number of iterations
   - tough : takes tens of minutes, large number of iterations, most reliable
    */
  mode = "default"
  jvmArgsPrepend = "-Djdk.stdout.sync=true"
}

jmh {
  jmhVersion.set(libs.versions.jmh.asProvider())
  warmupIterations = 5
  iterations = 10
  fork = 1
  threads = 10
  resultFormat = "csv"
  resultsFile = file("$buildDir/reports/jmh/results.csv")
}
