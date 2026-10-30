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

// This module supports Trino versions 482-483. Everything shared by the version-segment modules
// (toolchain, dependencies, Spotless, test wiring, distribution tasks) is configured once in
// segment.gradle; this file only declares the module's range and source directories.
extra["minTrinoVersion"] = 482
extra["maxTrinoVersion"] = 483
extra["defaultTrinoVersionIsMin"] = false

dependencies {
  // Trino 482+ serializes remote-task results using Jackson Blackbird for classes defined by
  // this plugin; every Trino-bundled plugin carries its own copy for the same reason, since the
  // plugin classloader does not see Trino's own copy of this module.
  runtimeOnly("com.fasterxml.jackson.module:jackson-module-blackbird:2.22.0")
}

sourceSets {
  main {
    java.srcDirs("../src/common/main/java", "../src/common-480-483/main/java", "../src/common-482-483/main/java")
  }
  test {
    java.srcDirs("../src/common/test/java", "../src/common-482-483/test/java")
    resources.srcDirs("../src/common/test/resources")
  }
}

apply(from = "../segment.gradle")
