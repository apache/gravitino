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
description = "lance-rest-server"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String =
  project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark35.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()
// Comma-separated list of lance-spark-bundle versions to test against.
// The default is the latest supported version; the integration test matrix
// (`:lance:lance-rest-server:lanceSparkMatrixTest`) covers every version in
// this list. Override via `-PlanceSparkBundleVersions=0.2.0,0.4.0`.
val lanceSparkBundleVersions: List<String> =
  ((project.properties["lanceSparkBundleVersions"] as? String) ?: "0.4.0")
    .split(",").map { it.trim() }.filter { it.isNotEmpty() }.distinct()
if (lanceSparkBundleVersions.isEmpty()) {
  throw GradleException("lanceSparkBundleVersions must contain at least one version")
}
val primaryLanceSparkBundleVersion: String = lanceSparkBundleVersions.first()
val lanceSparkBundleJarPathProperty = "gravitino.lance.spark.bundle.jar"

fun lanceSparkBundleConfigName(version: String): String =
  "lanceSparkBundle_" + version.replace(".", "_").replace("-", "_")
fun lanceSparkBundleDirFor(version: String) =
  layout.buildDirectory.dir("lance-spark-bundle/$version")
fun lanceSparkPrepareTaskName(version: String): String =
  "prepareLanceSparkBundle_" + version.replace(".", "_").replace("-", "_")
fun lanceSparkTestTaskName(version: String): String =
  "testLanceSparkBundle_" + version.replace(".", "_").replace("-", "_")

lanceSparkBundleVersions.forEach { version ->
  configurations.create(lanceSparkBundleConfigName(version)) {
    isCanBeConsumed = false
    isCanBeResolved = true
    isTransitive = false
  }
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":lance:lance-common"))
  implementation(project(":server-common")) {
    exclude("*")
  }

  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.jwt)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.metrics)
  implementation(libs.bundles.prometheus)

  constraints {
    implementation(libs.gson)
  }
  implementation(libs.commons.lang3)
  implementation(libs.lance.namespace.core) {
    exclude(group = "org.lance", module = "lance-core")
    exclude(group = "com.google.guava", module = "guava") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.core", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.datatype", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.jaxrs", module = "jackson-jaxrs-json-provider") // using gravitino's version
    exclude(group = "org.apache.commons", module = "commons-lang3") // provided by gravitino
    exclude(group = "org.apache.opendal", module = "*")
    exclude(group = "org.junit.jupiter", module = "*")
  }
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jackson.jaxrs.json.provider)
  implementation(libs.metrics.jersey2)

  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(libs.lance)

  lanceSparkBundleVersions.forEach { version ->
    add(
      lanceSparkBundleConfigName(version),
      "org.lance:lance-spark-bundle-3.5_2.12:$version"
    )
  }

  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }

  testImplementation(libs.awaitility)
  testImplementation(libs.commons.io)
  testImplementation(libs.hadoop3.client.api)
  testImplementation(libs.hadoop3.client.runtime)
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }
  // One Sync task per lance-spark-bundle version. Each task lays down its
  // bundle jar under build/lance-spark-bundle/<version>/ so per-version Test
  // tasks pick up the right jar without colliding.
  lanceSparkBundleVersions.forEach { version ->
    register<Sync>(lanceSparkPrepareTaskName(version)) {
      from(configurations.getByName(lanceSparkBundleConfigName(version)))
      into(lanceSparkBundleDirFor(version))
    }
  }
  val primaryPrepareLanceSparkBundle =
    named(lanceSparkPrepareTaskName(primaryLanceSparkBundleVersion))

  jar {
    finalizedBy(copyDepends)
  }

  register("copyLibs", Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs")
    into("$rootDir/distribution/package/lance-rest-server/libs")
  }

  register("copyLibsToStandalonePackage", Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs")
    into("$rootDir/distribution/gravitino-lance-rest-server/libs")
  }

  register("copyLibAndConfigs", Copy::class) {
    dependsOn("copyLibs")
  }

  register("copyLibAndConfigsToStandalonePackage", Copy::class) {
    dependsOn("copyLibsToStandalonePackage")
  }

  named("generateMetadataFileForMavenJavaPublication") {
    dependsOn(copyDepends)
  }

  test {
    dependsOn(primaryPrepareLanceSparkBundle)

    val primaryBundleDir = lanceSparkBundleDirFor(primaryLanceSparkBundleVersion)
    doFirst {
      val bundleJar =
        primaryBundleDir.get().asFile.listFiles()?.singleOrNull { it.extension == "jar" }
          ?: throw GradleException(
            "Expected exactly one Lance Spark bundle jar in ${primaryBundleDir.get().asFile}"
          )
      systemProperty(lanceSparkBundleJarPathProperty, bundleJar.absolutePath)
    }

    val testMode = project.properties["testMode"] as? String ?: "embedded"
    if (testMode == "embedded") {
      dependsOn(":catalogs:catalog-lakehouse-generic:jar")
    }
  }

  // Per-version Test task that only runs LanceSparkRESTServiceIT against a
  // specific lance-spark-bundle. Each task downloads its bundle through the
  // matching Sync task and points the IT JVM at it via system property.
  lanceSparkBundleVersions.forEach { version ->
    register<Test>(lanceSparkTestTaskName(version)) {
      group = "verification"
      description =
        "Run LanceSparkRESTServiceIT against lance-spark-bundle $version"

      dependsOn(named(lanceSparkPrepareTaskName(version)))
      dependsOn(named("jar"))
      val versionTestMode = project.properties["testMode"] as? String ?: "embedded"
      if (versionTestMode == "embedded") {
        dependsOn(":catalogs:catalog-lakehouse-generic:jar")
      }

      testClassesDirs = sourceSets["test"].output.classesDirs
      classpath = sourceSets["test"].runtimeClasspath
      useJUnitPlatform()
      filter { includeTestsMatching("*LanceSparkRESTServiceIT*") }

      val versionBundleDir = lanceSparkBundleDirFor(version)
      doFirst {
        val bundleJar =
          versionBundleDir.get().asFile.listFiles()?.singleOrNull { it.extension == "jar" }
            ?: throw GradleException(
              "Expected exactly one Lance Spark bundle jar in " +
                "${versionBundleDir.get().asFile} for version $version"
            )
        systemProperty(lanceSparkBundleJarPathProperty, bundleJar.absolutePath)
        println("[lance-spark-matrix] running IT against bundle $version -> ${bundleJar.name}")
      }

      // Send per-version reports to a separate directory so a matrix run
      // doesn't overwrite results across versions.
      val versionSlug = version.replace(".", "_").replace("-", "_")
      reports {
        html.outputLocation.set(
          layout.buildDirectory.dir("reports/lance-spark-matrix/$versionSlug")
        )
        junitXml.outputLocation.set(
          layout.buildDirectory.dir("test-results/lance-spark-matrix/$versionSlug")
        )
      }
    }
  }

  register("lanceSparkMatrixTest") {
    group = "verification"
    description =
      "Run LanceSparkRESTServiceIT against every version in -PlanceSparkBundleVersions " +
      "(default: $primaryLanceSparkBundleVersion). Reports land under " +
      "build/reports/lance-spark-matrix/<version>/."
    dependsOn(
      lanceSparkBundleVersions.map { named(lanceSparkTestTaskName(it)) }
    )
  }

  // Force serial execution: each version spins up an embedded MiniGravitino on a
  // dynamically chosen port. findAvailablePort is a scan, not an atomic OS-level
  // reservation (TOCTOU), so concurrent tasks can race to bind the same port and
  // produce intermittent "address already in use" failures under --parallel.
  lanceSparkBundleVersions.zipWithNext { a, b ->
    named(lanceSparkTestTaskName(b)) {
      mustRunAfter(named(lanceSparkTestTaskName(a)))
    }
  }
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}
