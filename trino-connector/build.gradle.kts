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

import net.ltgt.gradle.errorprone.errorprone

// This project builds nothing of its own; it groups the Trino version-segment modules and owns the
// shared shape source trees (common/, common-440-479/, common-440-481/, common-480-481/) that the
// modules compile through their sourceSets. Only Spotless stays enabled here.
tasks.all {
  enabled = name.startsWith("spotless")
}

// The shared shape trees belong to no module, so no module's Spotless picks them up. Format them
// from here, where they live, and keep the modules scoped to their own trees.
plugins.withId("com.diffplug.spotless") {
  configure<com.diffplug.gradle.spotless.SpotlessExtension> {
    java {
      target(
        project.fileTree("common") { include("**/*.java") },
        project.fileTree("common-440-479") { include("**/*.java") },
        project.fileTree("common-440-481") { include("**/*.java") },
        project.fileTree("common-480-481") { include("**/*.java") }
      )
    }
  }
}

// Error Prone is incompatible with the JDK 24/25 toolchains the Trino SPI requires, so it is
// disabled for every module below this project.
subprojects {
  tasks.withType<JavaCompile>().configureEach {
    options.errorprone.isEnabled.set(false)
  }
}
