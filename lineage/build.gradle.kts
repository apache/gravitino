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
  id("java")
}

dependencies {
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":server-common"))
  implementation(libs.bundles.jersey)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.metrics.jersey2)
  implementation(libs.openlineage.java) {
    isTransitive = false
  }
  implementation(libs.slf4j.api)
  implementation(libs.httpclient5)
  implementation(libs.httpcore5)
  implementation(libs.jackson.dataformat.yaml)
  implementation(libs.micrometer.core)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(libs.awaitility)
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)

  testRuntimeOnly(libs.junit.jupiter.engine)
}
