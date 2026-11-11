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

description = "hadoop-auth"

plugins {
  id("java")
}

dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":common"))

  compileOnly(libs.hadoop3.client.api)
  compileOnly(libs.guava)
  compileOnly(libs.slf4j.api)

  testImplementation(project(":api"))
  testImplementation(project(":common"))

  testImplementation(libs.hadoop3.client.api)
  testImplementation(libs.hadoop3.client.runtime)
  testImplementation(libs.guava)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.slf4j.api)

  testRuntimeOnly(libs.junit.jupiter.engine)
}
