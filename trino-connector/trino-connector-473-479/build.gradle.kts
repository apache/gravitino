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

// This module supports Trino versions 473-479. Everything shared by the version-segment modules
// (toolchain, dependencies, Spotless, test wiring, distribution tasks) is configured once in
// segment.gradle.kts; this file only declares the module's range and source directories.
extra["minTrinoVersion"] = 473
extra["maxTrinoVersion"] = 479
extra["otelSemconvVersion"] = "1.32.0"
extra["defaultTrinoVersionIsMin"] = false

sourceSets {
  main {
    java.srcDirs(
      "../common/src/main/java",
      "../common-440-479/src/main/java",
      "../common-440-481/src/main/java"
    )
  }
  test {
    java.srcDirs(
      "../common/src/test/java",
      "../common-440-481/src/test/java"
    )
    resources.srcDirs("../common/src/test/resources")
  }
}

apply(from = "../segment.gradle")
