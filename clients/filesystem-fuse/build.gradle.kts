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

import io.github.liurenjie1024.gradle.rust.CargoBuildTask
import io.github.liurenjie1024.gradle.rust.CargoCleanTask
import io.github.liurenjie1024.gradle.rust.CargoDocTask
import io.github.liurenjie1024.gradle.rust.CargoTestTask
import io.github.liurenjie1024.gradle.rust.FeatureSpec as CargoFeatureSpec

plugins {
  id("io.github.liurenjie1024.gradle.rust") version "0.1.0"
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

tasks.withType(CargoBuildTask::class.java).configureEach {
  dependsOn(checkRustProject)
  verbose = false
  release = true
  extraCargoBuildArguments = listOf("--workspace")
  featureSpec = CargoFeatureSpec.all()
}

val testRustProject = tasks.withType(CargoTestTask::class.java)
testRustProject.configureEach {
  dependsOn(checkRustProject)
  extraCargoBuildArguments = listOf("--no-fail-fast", "--all-targets", "--all-features", "--workspace")
}

tasks.withType(CargoCleanTask::class.java).configureEach {
  dependsOn(checkRustProject)
}

tasks.withType(CargoDocTask::class.java).configureEach {
  dependsOn(checkRustProject)
}

tasks.named("test") {
  dependsOn(testRustProject)
}

tasks.named("check") {
  dependsOn.clear()
  dependsOn(checkRustProject)
}
