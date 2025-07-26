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
description = "catalog-model-it"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val isTestModeEmbedded = rootProject.extra["isTestModeEmbedded"] as Boolean

dependencies {
  testImplementation(project(":api")) {
    exclude(group = "*")
  }
  testImplementation(project(":catalogs:catalog-common")) {
    exclude(group = "*")
  }
  testImplementation(project(":common")) {
    exclude(group = "*")
  }
  testImplementation(project(":core")) {
    exclude(group = "*")
  }
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  if (isTestModeEmbedded) {
    testImplementation(project(":server"))
  }
  testImplementation(project(":server-common"))

  testImplementation(libs.bundles.log4j)
  testImplementation(libs.commons.io)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.guava)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.slf4j.api)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
    dependsOn(":catalogs:catalog-model:jar")
  }
}
