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
description = "client-java-it"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val isTestModeEmbedded = rootProject.extra["isTestModeEmbedded"] as Boolean

dependencies {
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  if (isTestModeEmbedded) {
    testImplementation(project(":server"))
  }
  testImplementation(project(":server-common"))

  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.hadoop3.client)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.minikdc)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockserver.netty)
  testImplementation(libs.mockserver.client.java)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    dependsOn(":catalogs:catalog-fileset:jar", ":catalogs:catalog-fileset:runtimeJars")
    dependsOn(":catalogs:catalog-hive:jar", ":catalogs:catalog-hive:runtimeJars")
    dependsOn(":catalogs:catalog-kafka:jar", ":catalogs:catalog-kafka:runtimeJars")
  }
}

tasks.clean {
  delete("target")
  delete("tmp")
}
