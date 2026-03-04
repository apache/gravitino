/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
description = "lance-common"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":clients:client-java"))
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":core")) {
    exclude("*")
  }

  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jackson.jaxrs.json.provider)
  implementation(libs.lance.namespace.core) {
    exclude(group = "com.lancedb", module = "lance-core")
    exclude(group = "com.google.guava", module = "guava") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.core", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.datatype", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.jaxrs", module = "jackson-jaxrs-json-provider") // using gravitino's version
    exclude(group = "org.apache.commons", module = "commons-lang3") // provided by gravitino
    exclude(group = "org.apache.opendal", module = "*")
    exclude(group = "org.junit.jupiter", module = "*")
  }
  implementation(libs.slf4j.api)

  testImplementation(project(":server-common"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}
