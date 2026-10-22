import net.ltgt.gradle.errorprone.errorprone

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
  testImplementation(libs.junit.platform.testkit)
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
    systemProperty(coreTestBackendProperty, backend)
    // Read by the root setTestEnvironment inside doFirst, i.e. at execution time, after every
    // configuration action has run - so it doesn't depend on register/configureEach ordering.
    extensions.extraProperties["includeDockerTaggedTests"] = true

    // Database tests mutate process-wide state and must remain sequential within each lane.
    maxParallelForks = 1
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")

    if (backend != "h2") {
      // The test databases are thrown away, so their containers can skip durability work.
      // Read by DatabaseContainer.Durability.fromSystemProperty(); keep the name in sync.
      systemProperty("gravitino.test.db.disposableData", "true")
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

// Nested fixture classes TestBackendTestExtension runs through EngineTestKit; Gradle's class scan
// would run them directly too. Keep in sync with TestBackendTestExtension.FIXTURE.
tasks.withType<Test>().configureEach {
  useJUnitPlatform { excludeTags("gravitino-test-fixture") }
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
