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

val enablePgvfsFuse = project.hasProperty("enable_gvfs_fuse")

if (enablePgvfsFuse) {
  val checkRustEnvironment by tasks.registering(Exec::class) {
    description = "Check if Rust environment."
    group = "verification"
    commandLine("bash", "-c", "cargo --version")
    standardOutput = System.out
    errorOutput = System.err
    isIgnoreExitValue = false
  }

  val compileRust by tasks.registering(Exec::class) {
    dependsOn(checkRustEnvironment)
    description = "Compile the Rust project"
    workingDir = file("$projectDir")

    commandLine(
      "bash",
      "-c",
      """
          set -e
          echo "Checking the code format"
          cargo fmt --all -- --check

          echo "Running clippy"
          cargo clippy --all-targets --all-features --workspace -- -D warnings

          echo "Compiling Rust project"
          cargo build --release
      """.trimIndent()
    )
  }

  val testRust by tasks.registering(Exec::class) {
    dependsOn(checkRustEnvironment)
    description = "Run tests in the Rust project"
    group = "verification"
    workingDir = file("$projectDir")
    commandLine("bash", "-c", "cargo test --release")

    standardOutput = System.out
    errorOutput = System.err
  }

  tasks.named("build") {
    dependsOn(compileRust)
  }
  tasks.named("test") {
    dependsOn(testRust)
  }
} else {
  println("Skipping Gvfs-fuse tasks since -Penable_gvfs_fuse is not enabled.")
}
