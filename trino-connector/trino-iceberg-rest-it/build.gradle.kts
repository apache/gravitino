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

// Test against the highest supported Trino version.
val minTrinoVersion = 473
val maxTrinoVersion = 478
val trinoVersion = providers.gradleProperty("trinoVersion")
  .map { it.trim().toInt() }
  .orElse(maxTrinoVersion)
  .get()

check(trinoVersion in minTrinoVersion..maxTrinoVersion) {
  "Module ${project.path} supports Trino versions $minTrinoVersion-$maxTrinoVersion, " +
    "but trinoVersion=$trinoVersion was specified."
}

java {
  toolchain.languageVersion.set(JavaLanguageVersion.of(24))
}

dependencies {
  testImplementation("io.trino:trino-iceberg:$trinoVersion")
  testImplementation("io.trino:trino-testing:$trinoVersion")
  testImplementation("io.trino:trino-main:$trinoVersion")

  testImplementation(project(":integration-test-common", "testArtifacts"))

  // jjwt is resolved to 0.13.x on this module's test classpath (Trino forces it up). Only pull
  // api + impl; the JSON serializer comes from jjwt-jackson:0.13.x which Trino already provides.
  // Do NOT pull jjwt-gson:0.11.x (the bundle's default) — its stale 0.11.x serializer service is
  // incompatible with the 0.13.x runtime and makes signWith().compact() fail.
  testImplementation(libs.jwt.api)
  testRuntimeOnly(libs.jwt.impl)
  testImplementation(libs.guava)
  testImplementation(libs.commons.lang3)
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
  useJUnitPlatform()
  // These ITs require the built distribution (deploy mode) and a server subprocess.
  val skipITs = project.hasProperty("skipITs")
  val skipTests = project.hasProperty("skipTests")
  onlyIf { !skipITs && !skipTests }
}
