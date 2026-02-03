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
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

dependencies {
  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))

  testImplementation(libs.awaitility)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.commons.io)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.guava)
  testImplementation(libs.hadoop3.client)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.rauschig)
  testImplementation(libs.selenium)
  testImplementation(libs.testcontainers)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("*")
  } else {
    dependsOn(":trino-connector:trino-connector:jar")
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar", ":catalogs:catalog-lakehouse-iceberg:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-doris:jar", ":catalogs:catalog-jdbc-doris:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-mysql:jar", ":catalogs:catalog-jdbc-mysql:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-postgresql:jar", ":catalogs:catalog-jdbc-postgresql:runtimeJars")
    dependsOn(":catalogs:catalog-fileset:jar", ":catalogs:catalog-fileset:runtimeJars")
    dependsOn(":catalogs:catalog-hive:jar", ":catalogs:catalog-hive:runtimeJars")
    dependsOn(":catalogs:catalog-kafka:jar", ":catalogs:catalog-kafka:runtimeJars")

    // Frontend tests depend on the web page, so we need to build the web module first.
    dependsOn(":web:web:build")
  }
}
