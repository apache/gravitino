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
  compileOnly(libs.hadoop3.common)
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(libs.caffeine)

  testImplementation(project(":core"))
  testImplementation(project(":server-common"))
  testImplementation(libs.awaitility)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.hadoop3.common)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.minikdc)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockserver.netty) {
    exclude("com.google.guava", "guava")
  }
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.build {
  dependsOn("javadoc")
}

tasks.javadoc {
  dependsOn(":clients:client-java-runtime:javadoc")
  source = sourceSets["main"].allJava +
    project(":clients:client-java-runtime").sourceSets["main"].allJava
}
