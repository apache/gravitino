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

import org.gradle.api.tasks.Exec

val checkRustEnvironment by tasks.registering(Exec::class) {
  description = "Check if Rust environment."
  group = "verification"
  commandLine("bash", "-c", "cargo --version")
  standardOutput = System.out
  errorOutput = System.err
  isIgnoreExitValue = false
}

val buildRustProject by tasks.registering(Exec::class) {
  dependsOn(checkRustEnvironment)
  description = "Compile the Rust project"
  workingDir = file("$projectDir")
  commandLine("bash", "-c", "cargo build --release")
}

val checkRustProject by tasks.registering(Exec::class) {
  dependsOn(checkRustEnvironment)
  description = "Check the Rust project"
  workingDir = file("$projectDir")

  commandLine(
    "bash",
    "-c",
    """
          set -e
          echo "Checking the code format"
          cargo fmt --all -- --check

          echo "Running clippy"
          #cargo clippy --all-targets --all-features --workspace -- -D warnings
          cargo clippy --all-targets --all-features --workspace -- 
    """.trimIndent()
  )
}

val testRustProject by tasks.registering(Exec::class) {
  dependsOn(checkRustEnvironment)
  description = "Run tests in the Rust project"
  group = "verification"
  workingDir = file("$projectDir")
  commandLine("bash", "-c", "cargo test --release")

  standardOutput = System.out
  errorOutput = System.err
}

tasks.named("testRustProject") {
  mustRunAfter("checkRustProject")
}
tasks.named("buildRustProject") {
  mustRunAfter("testRustProject")
}

tasks.named("build") {
  dependsOn(testRustProject)
  dependsOn(buildRustProject)
}

tasks.named("check") {
  dependsOn.clear()
  dependsOn(checkRustProject)
}

tasks.named("test") {
  dependsOn(testRustProject)
}
