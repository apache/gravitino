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
description = "authorization-kafka"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":common"))
  compileOnly(project(":core"))
  implementation(project(":authorizations:authorization-common")) {
    exclude(group = "*")
  }
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.kafka.clients) {
    // AdminClient ACL control-plane calls do not compress record batches.
    exclude(group = "at.yawk.lz4", module = "lz4-java")
    exclude(group = "com.github.luben", module = "zstd-jni")
    exclude(group = "org.lz4", module = "lz4-java")
    exclude(group = "org.xerial.snappy", module = "snappy-java")
  }
  implementation(libs.slf4j.api)

  testImplementation(project(":api"))
  testImplementation(project(":authorizations:authorization-common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.awaitility)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath) {
      exclude("error_prone_annotations-*.jar")
    }
    into("build/libs")
  }

  val copyAuthorizationLibs by registering(Copy::class) {
    dependsOn("jar", runtimeJars)
    from("build/libs") {
      exclude("guava-*.jar")
      exclude("log4j-*.jar")
      exclude("slf4j-*.jar")
      exclude("error_prone_annotations-*.jar")
    }
    into("$rootDir/distribution/package/authorizations/kafka/libs")
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyAuthorizationLibs)
  }

  jar {
    dependsOn(runtimeJars)
  }
}

tasks.test {
  dependsOn(":catalogs:catalog-kafka:jar", ":catalogs:catalog-kafka:runtimeJars")

  val skipDockerTests =
    project.findProperty("skipDockerTests")?.toString()?.toBoolean() ?: true
  if (!skipDockerTests) {
    // KafkaAuthorizationIT binds its embedded KRaft broker to 127.0.0.1 and does not require
    // container-IP routing on macOS when -PskipDockerTests=false is explicitly requested.
    extensions.extraProperties["includeDockerTaggedTests"] = true
  }

  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}
