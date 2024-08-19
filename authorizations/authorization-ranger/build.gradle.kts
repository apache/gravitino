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
description = "authorization-ranger"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api")) {
    exclude(group = "*")
  }
  implementation(project(":core")) {
    exclude(group = "*")
  }
  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.lombok)
  implementation(libs.jackson.annotations)
  implementation(libs.ranger.intg) {
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
    exclude("org.apache.ranger", "ranger-plugins-audit")
    exclude("org.apache.ranger", "ranger-plugins-cred")
    exclude("org.apache.ranger", "ranger-plugins-classloader")
  }
  implementation(libs.javax.jaxb.api) {
    exclude("*")
  }

  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testImplementation(libs.testcontainers)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.ranger.intg) {
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
  }
  testImplementation(libs.hive2.jdbc) {
    exclude("org.slf4j")
  }
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }
}

tasks.build {
  dependsOn("runtimeJars", "javadoc")
}

tasks.test {
  val skipUTs = project.hasProperty("skipTests")
  if (skipUTs) {
    // Only run integration tests
    include("**/integration/**")
  }

  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/**")
  } else {
    dependsOn(tasks.jar)

    doFirst {
      environment("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "datastrato/gravitino-ci-hive:0.1.13")
      environment("GRAVITINO_CI_RANGER_DOCKER_IMAGE", "datastrato/gravitino-ci-ranger:0.1.1")
    }

    val init = project.extra.get("initIntegrationTest") as (Test) -> Unit
    init(this)
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("runtimeJars")
}
