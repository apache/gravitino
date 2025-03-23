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

import io.github.liurenjie1024.gradle.rust.FeatureSpec as CargoFeatureSpec

plugins {
  id("io.github.liurenjie1024.gradle.rust") version "0.1.0"
}

tasks.withType(io.github.liurenjie1024.gradle.rust.CargoBuildTask::class.java).configureEach {
  verbose = false
  release = true
  extraCargoBuildArguments = listOf("--workspace")
  featureSpec = CargoFeatureSpec.all()
}

val checkRustEnvironment by tasks.registering(Exec::class) {
  commandLine("bash", "-c", "cargo --version")
  standardOutput = System.out
  errorOutput = System.err
  isIgnoreExitValue = false
}

val checkRustProject by tasks.registering(Exec::class) {
  dependsOn(checkRustEnvironment)
  workingDir = file("$projectDir")

  commandLine("bash", "-c", "make check")
}

tasks.named("check") {
  dependsOn.clear()
  dependsOn(checkRustProject)
}
