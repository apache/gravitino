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
description = "credential-s3"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.guava)

  annotationProcessor(libs.lombok)

  // todo: change to compile only
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:iam")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:kms")

  // compileOnly(platform(libs.awssdk.bom))
  // compileOnly("software.amazon.awssdk:iam")
  // compileOnly("software.amazon.awssdk:iam-policy-builder")
  // compileOnly("software.amazon.awssdk:s3")
  // compileOnly("software.amazon.awssdk:sts")
  // compileOnly("software.amazon.awssdk:kms")

  compileOnly(libs.lombok)

  testImplementation(platform(libs.awssdk.bom))
  testImplementation("software.amazon.awssdk:iam")
  testImplementation("software.amazon.awssdk:iam-policy-builder")
  testImplementation("software.amazon.awssdk:s3")
  testImplementation("software.amazon.awssdk:sts")
  testImplementation("software.amazon.awssdk:kms")

  testImplementation(libs.mockito.core)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.slf4j.api)

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

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("copyDepends")
}
