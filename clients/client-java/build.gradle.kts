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
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common")) {
    exclude(group = "org.apache.logging.log4j")
  }
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.guava)
  implementation(libs.httpclient5) {
    exclude(group = "org.slf4j")
  }
  implementation(libs.commons.lang3)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(project(":core"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
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

tasks.build {
  dependsOn("javadoc")
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    dependsOn(":catalogs:catalog-fileset:jar", ":catalogs:catalog-fileset:runtimeJars")
    dependsOn(":catalogs:catalog-model:jar", ":catalogs:catalog-model:runtimeJars")
    dependsOn(":catalogs:catalog-hive:jar", ":catalogs:catalog-hive:runtimeJars")
    dependsOn(":catalogs:catalog-kafka:jar", ":catalogs:catalog-kafka:runtimeJars")
  }
}

tasks.javadoc {
  dependsOn(":api:javadoc", ":common:javadoc")
  source =
    sourceSets["main"].allJava +
    project(":api").sourceSets["main"].allJava +
    project(":common").sourceSets["main"].allJava

  classpath = configurations["compileClasspath"] +
    project(":api").configurations["runtimeClasspath"] +
    project(":common").configurations["runtimeClasspath"]
}

tasks.clean {
  delete("target")
  delete("tmp")
}
