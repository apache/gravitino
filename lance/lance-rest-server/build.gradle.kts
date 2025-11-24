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

dependencies {
  implementation(project(":api"))
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":server-common")) {
    exclude("*")
  }

  implementation(project(":lance:lance-common"))
  implementation(libs.lance)
  implementation(libs.commons.lang3)

  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.metrics)
  implementation(libs.bundles.prometheus)
  implementation(libs.commons.lang3)
  implementation(libs.lance.namespace.core) {
    exclude(group = "com.lancedb", module = "lance-core")
  }
  implementation(libs.metrics.jersey2)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)

  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":integration-test-common", "testArtifacts"))

  testImplementation(libs.commons.io)
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
    val testMode = project.properties["testMode"] as? String ?: "embedded"
    if (testMode == "embedded") {
      dependsOn(":catalogs:catalog-generic-lakehouse:build")
    }
  }
}
