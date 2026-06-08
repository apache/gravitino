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

  testImplementation(libs.bundles.jwt)
  testImplementation(libs.guava)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.slf4j.api)
  testImplementation(platform("org.junit:junit-bom:5.9.1"))
  testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
  useJUnitPlatform()
  // These ITs require the built distribution (deploy mode) and a server subprocess.
  val skipITs = project.hasProperty("skipITs")
  val skipTests = project.hasProperty("skipTests")
  onlyIf { !skipITs && !skipTests }
}
