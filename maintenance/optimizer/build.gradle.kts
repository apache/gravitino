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
description = "Gravitino Optimizer"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String =
  project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
// Match maintenance/jobs and catalog Iceberg 1.11 so IsolatedClassLoader's shared
// org.apache.iceberg.* types include NestedField default APIs used by lakehouse-iceberg.
val sparkVersion: String = libs.versions.spark35.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4spark35.get()

dependencies {
  implementation(project(":api"))
  implementation(project(":maintenance:optimizer-api"))
  implementation(project(":maintenance:updaters"))
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":clients:client-java"))
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(libs.bundles.log4j)
  implementation(libs.commons.cli.new)
  implementation(libs.commons.lang3)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.guava)
  implementation(libs.commons.dbcp2)
  implementation(libs.ql.expression)
  implementation(libs.h2db)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.awaitility)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation("org.slf4j:slf4j-api:1.7.36")
  testRuntimeOnly("org.slf4j:slf4j-simple:1.7.36")
  testRuntimeOnly(
    "org.scala-lang.modules:scala-collection-compat_$scalaVersion:${libs.versions.scala.collection.compat.get()}"
  )
  testImplementation(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion"
  ) {
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
  }
  testImplementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion") {
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
  }
  testImplementation("org.apache.spark:spark-core_$scalaVersion:$sparkVersion") {
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
  }
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
  }
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":catalogs:catalog-lakehouse-iceberg"))
  testImplementation(project(":server"))
  testRuntimeOnly(libs.mysql.driver)
  testRuntimeOnly(libs.postgresql.driver)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

/**
 * Mirrors root [copySubprojectDependencies]: jars already shipped under distribution/package/libs
 * via those projects' runtimeClasspath. Optimizer packaging skips them and leaves them to
 * gravitino-optimizer.sh's addJarInDir of ${GRAVITINO_HOME}/libs.
 */
fun Project.publishesRuntimeJarsToRootLibs(): Boolean {
  val parentName = parent?.name
  return !name.startsWith("authorization") &&
    !name.startsWith("catalog") &&
    !name.startsWith("cli") &&
    !name.startsWith("client") &&
    !name.startsWith("filesystem") &&
    !name.startsWith("flink") &&
    !name.startsWith("iceberg") &&
    !name.startsWith("lance") &&
    !name.startsWith("optimizer") &&
    !name.startsWith("spark") &&
    !name.startsWith("hive-metastore") &&
    !name.startsWith("trino-connector") &&
    name != "hadoop-common" &&
    name != "integration-test" &&
    parentName != "bundles" &&
    parentName != "maintenance" &&
    parentName != "plugins" &&
    name != "mcp-server"
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }
  jar {
    finalizedBy(copyDepends)
  }

  // copyDepends fills build/libs; copyLibs keeps only jars that are NOT already published to root
  // libs. gravitino-optimizer.sh loads optimizer/libs then libs/ via addJarInDir — no shell
  // allowlist to update when Gradle dependencies change.
  register("copyLibs", Copy::class) {
    dependsOn(copyDepends, "build")
    // Callable so root-lib jar names are resolved at execution time, not configuration time.
    from({
      val rootLibJarNames =
        rootProject.subprojects
          .filter { it.publishesRuntimeJarsToRootLibs() }
          .flatMapTo(sortedSetOf()) { sp ->
            val runtime =
              sp.configurations.findByName("runtimeClasspath") ?: return@flatMapTo emptySet()
            if (!runtime.isCanBeResolved) {
              return@flatMapTo emptySet()
            }
            runtime.incoming.artifacts.artifacts.map { it.file.name }
          }
      fileTree(layout.buildDirectory.dir("libs")) {
        include("*.jar")
        exclude(
          "*-tests.jar",
          "*-sources.jar",
          "*-javadoc.jar",
          "*-jcstress.jar",
          "*-jmh.jar",
          "error_prone_annotations-*.jar"
        )
        // Always keep this module's jar; skip anything root libs already ships.
        exclude { element ->
          val name = element.file.name
          !name.startsWith("gravitino-optimizer-") && rootLibJarNames.contains(name)
        }
      }
    })
    into("$rootDir/distribution/package/optimizer/libs")
  }

  register("copyLibAndConfigs", Copy::class) {
    dependsOn("copyLibs")
  }
}

configurations.testRuntimeClasspath {
  exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j2-impl")
}

// The lakehouse-iceberg catalog packages runtime jars beside its module jar in build/libs
// (same pattern as jobs → optimizer:copyDepends). Order compileTestJava after that.
tasks.compileTestJava {
  dependsOn(":catalogs:catalog-lakehouse-iceberg:jar", ":catalogs:catalog-lakehouse-iceberg:runtimeJars")
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
    dependsOn(":server:jar")
    // Embedded optimizer integration tests load the Iceberg catalog in an isolated classloader.
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar")
    dependsOn(":catalogs:catalog-lakehouse-iceberg:runtimeJars")
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("copyDepends")
}
