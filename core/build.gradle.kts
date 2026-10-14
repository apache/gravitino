import com.sun.management.OperatingSystemMXBean
import net.ltgt.gradle.errorprone.errorprone
import org.apache.gravitino.testing.CoreDatabaseConcurrency
import org.apache.gravitino.testing.CoreDatabaseConcurrency.DetectedCapacity
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

// Core's tests run in four lanes: coreUnitTest, plus coreH2Test/coreMySQLTest/corePostgreSQLTest.
// A class joins a backend lane through its @CoreBackend.* annotation; see CoreBackend.java.
// Backend name -> the JUnit tag that admits a class to that lane. Keep in sync with CoreBackend.
val coreBackendTestTags =
  linkedMapOf(
    "h2" to "gravitino-core-h2-test",
    "mysql" to "gravitino-core-mysql-test",
    "postgresql" to "gravitino-core-postgresql-test"
  )
val coreTestBackendProperty = "gravitino.core.test.backend"
val coreDatabaseTaskNames =
  setOf("coreH2Test", "coreMySQLTest", "corePostgreSQLTest")
val coreDatabaseActiveLaneCount =
  gradle.startParameter.taskNames
    .map { it.substringAfterLast(':') }
    .distinct()
    .count { it in coreDatabaseTaskNames }
    .coerceAtLeast(1)
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

fun registerCoreTestTask(
  taskName: String,
  backend: String? = null
) = tasks.register<Test>(taskName) {
  group = "verification"
  description =
    if (backend == null) {
      "Runs core unit tests."
    } else {
      "Runs core database tests against $backend."
    }

  testClassesDirs = sourceSets["test"].output.classesDirs
  classpath = sourceSets["test"].runtimeClasspath

  reports.html.outputLocation.set(
    rootProject.layout.buildDirectory.dir("reports/tests/core/$taskName")
  )

  useJUnitPlatform {
    if (backend == null) {
      // Whatever carries no backend tag (and no Docker tag) is the unit suite.
      excludeTags(*coreBackendTestTags.values.toTypedArray(), "gravitino-docker-test")
    } else {
      val ownBackendTag =
        coreBackendTestTags[backend]
          ?: throw GradleException("Unsupported core test backend: $backend")
      // Plain tag include, applied by JUnit at discovery time, so classes not tagged for this
      // backend never show up in this lane's JUnit XML. A class tagged for several backends
      // runs under each of them.
      includeTags(ownBackendTag)
    }
  }

  if (backend != null) {
    val shardTelemetryDirectory =
      layout.buildDirectory.dir("test-results/$taskName/shard-telemetry")
    val laneMaximum =
      CoreDatabaseConcurrency.laneMaximum(
        backend != "h2",
        macDockerConnectorFixedNetwork
      )
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

    if (backend != "h2") {
      doFirst {
        if (rootProject.extra["dockerTest"] != true) {
          throw GradleException(
            "$path requires Docker; use -PskipDockerTests=false with Docker running."
          )
        }
      }
    }
  }
}

registerCoreTestTask("coreUnitTest")
registerCoreTestTask("coreH2Test", "h2")
registerCoreTestTask("coreMySQLTest", "mysql")
registerCoreTestTask("corePostgreSQLTest", "postgresql")

val coreSuiteCoverage =
  providers.gradleProperty("coreSuiteCoverage").map(String::toBoolean).orElse(false)
val coreSuiteTaskNames =
  listOf("coreUnitTest", "coreH2Test", "coreMySQLTest", "corePostgreSQLTest")
val coreSuiteExecutionData =
  coreSuiteTaskNames.map { layout.buildDirectory.file("jacoco/$it.exec") }
tasks.named<JacocoReport>("jacocoTestReport") {
  if (coreSuiteCoverage.get()) {
    dependsOn(tasks.named("classes"))
    executionData.setFrom(coreSuiteExecutionData)
  }
}

// :core:test is kept for IDEs and local habit, and is deprecated: run a lane instead. CI skips it
// (`build` passes `-x :core:test`) and runs each lane in its own `core-test` job.
// TestRestrictedRegoGoldenFile reads docs/policies.md. It is a unit test, so only coreUnitTest and
// the legacy :core:test run it; the database lanes don't need the input.
listOf("coreUnitTest", "test").forEach {
  tasks.named<Test>(it) {
    inputs.file(rootProject.file("docs/policies.md"))
    systemProperty("gravitino.repositoryRoot", rootProject.projectDir.path)
  }
}

tasks.test {
  doFirst {
    logger.warn(
      "WARNING: :core:test is deprecated and does not correspond to any CI lane. Use " +
        "coreUnitTest, coreH2Test, coreMySQLTest, or corePostgreSQLTest instead."
    )
  }
  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode == "embedded") {
    environment("GRAVITINO_HOME", project.rootDir.path)
  } else {
    environment("GRAVITINO_HOME", project.rootDir.path + "/distribution/package")
  }
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
