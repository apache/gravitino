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
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(libs.bcprov.jdk18on)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.h2db)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.mybatis)
  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.postgresql.driver)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  test {
    environment("GRAVITINO_HOME", rootDir.path)
    environment("GRAVITINO_TEST", "true")
  }
}
