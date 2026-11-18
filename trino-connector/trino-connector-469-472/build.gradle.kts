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
  `java-library`
  `maven-publish`
}

// This module supports Trino versions 469-472. Everything shared by the version-segment modules
// (toolchain, dependencies, Spotless, test wiring, distribution tasks) is configured once in
// segment.gradle; this file only declares the module's range and source directories.
extra["minTrinoVersion"] = 469
extra["maxTrinoVersion"] = 472
extra["defaultTrinoVersionIsMin"] = true

dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":clients:client-java"))
  compileOnly(project(":common"))
}

sourceSets {
  main {
    java.srcDirs("../src/common/main/java", "../src/common-469-481/main/java", "../src/common-469-479/main/java")
  }
  test {
    java.srcDirs("../src/common/test/java", "../src/common-469-481/test/java", "../src/common-469-479/test/java")
    resources.srcDirs("../src/common/test/resources")
  }
}

apply(from = "../segment.gradle")
