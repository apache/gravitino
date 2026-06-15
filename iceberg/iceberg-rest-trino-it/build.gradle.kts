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
import net.ltgt.gradle.errorprone.errorprone

plugins {
  id("java")
}

// Versions are centrally managed in gradle/libs.versions.toml.
val testcontainersVersion: String = libs.versions.testcontainers.get()
val dockerJavaVersion: String = libs.versions.docker.java.get()

// This test-only module boots an in-process Trino query runner, so it only needs to run against a
// single Trino version. Unlike the trino-connector modules (which must support a range of
// versions), there is no need for the multi-version range machinery here.
val trinoVersion: String = libs.versions.trino.iceberg.it.get()

configurations.configureEach {
  resolutionStrategy.force(
    "org.testcontainers:testcontainers:$testcontainersVersion",
    "com.github.docker-java:docker-java-api:$dockerJavaVersion",
    "com.github.docker-java:docker-java-transport:$dockerJavaVersion",
    "com.github.docker-java:docker-java-transport-zerodep:$dockerJavaVersion"
  )
  exclude(group = "io.jsonwebtoken", module = "jjwt-gson")
  exclude(group = "org.apache.logging.log4j", module = "log4j-to-slf4j")
}

java {
  toolchain.languageVersion.set(JavaLanguageVersion.of(24))
}

dependencies {
  testImplementation(project(":api"))
  testImplementation(project(":catalogs:catalog-common"))
  testImplementation(project(":common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":core"))
  testImplementation(project(":server-common"))

  testImplementation("io.trino:trino-iceberg:$trinoVersion")
  testImplementation("io.trino:trino-hdfs:$trinoVersion")
  testImplementation("io.trino:trino-testing:$trinoVersion")
  testImplementation("io.trino:trino-main:$trinoVersion")

  testImplementation(project(":integration-test-common", "testArtifacts"))

  // Runtime dependencies of the inherited BaseIT/ITUtils harness. The testArtifacts dependency
  // above only carries their compiled classes, not their transitive runtime deps, so these must be
  // declared here (BaseIT uses Awaitility and commons-lang3; omitting them fails the ITs at runtime
  // with NoClassDefFoundError).
  testRuntimeOnly(libs.awaitility)
  testRuntimeOnly(libs.commons.lang3)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.postgresql)

  // jjwt is resolved to 0.13.x on this module's test classpath (Trino forces it up). Only pull
  // api + impl; the JSON serializer (jjwt-jackson:0.13.x) is already provided transitively by
  // Trino, so it does not need to be declared here. Do NOT pull jjwt-gson:0.11.x (the bundle's
  // default) — its stale 0.11.x serializer service is incompatible with the 0.13.x runtime and
  // makes signWith().compact() fail.
  testImplementation(libs.jwt.api)
  testRuntimeOnly(libs.jwt.impl)
  testImplementation(libs.guava)
  testImplementation(libs.slf4j.api)
  // Trino 478's test stack forces JUnit to 6.0.0; pin the BOM to match so the platform launcher,
  // engine and commons jars stay on the same version (a mismatch makes Gradle's TagFilter crash
  // with NoSuchMethodError on CollectionUtils.toUnmodifiableList).
  testImplementation(platform("org.junit:junit-bom:6.0.0"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile>().configureEach {
  // Error Prone (2.10.0) is incompatible with the JDK 24 toolchain required by this Trino range.
  options.errorprone.isEnabled.set(false)
}

tasks.withType<Test>().configureEach {
  // JaCoCo 0.8.9 cannot instrument classes compiled for the JDK 24 toolchain used here.
  extensions
    .findByType(org.gradle.testing.jacoco.plugins.JacocoTaskExtension::class.java)
    ?.isEnabled = false
}

tasks.test {
  // This module contains only integration tests that require the built distribution (deploy
  // mode). Exclude them when integration tests are skipped, consistent with the other modules.
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  }
}
