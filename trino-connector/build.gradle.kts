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

import com.diffplug.gradle.spotless.SpotlessExtension

// This project builds nothing of its own; it groups the per-Trino-version modules and owns the
// src/common and src/common-* source trees they all pull in via sourceSets.srcDirs, none of
// which is itself a buildable module. Only Spotless stays enabled, to format those trees.
tasks.all {
  enabled = name.startsWith("spotless")
}

// These trees belong to no module, so no module's Spotless picks them up. Format them from here,
// where they live, and keep the version modules scoped to their own trees.
plugins.withId("com.diffplug.spotless") {
  configure<SpotlessExtension> {
    java {
      target(project.fileTree(".") { include("src/common/**/*.java", "src/common-*/**/*.java") })
    }
  }
}
