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

val envFile = System.getenv("HOME") + "/.cargo/env"

val checkRustEnvironment by tasks.registering(Exec::class) {
  description = "Check if Rust environment is properly set up using an external script"
  group = "verification"
  commandLine("bash", "$projectDir/check_rust_env.sh")
  isIgnoreExitValue = false
}

val compileRust by tasks.registering(Exec::class) {
  dependsOn(checkRustEnvironment)
  description = "Compile the Rust project"
  workingDir = file("$projectDir")
  commandLine("bash", "-c", ". $envFile && cargo build --release")
}

val testRust by tasks.registering(Exec::class) {
  dependsOn(checkRustEnvironment)
  description = "Run tests in the Rust project"
  group = "verification"
  workingDir = file("$projectDir")
  commandLine("bash", "-c", ". $envFile && cargo test --release")

  standardOutput = System.out
  errorOutput = System.err
}

tasks.named("build") {
  dependsOn(compileRust)
}
tasks.named("test") {
  dependsOn(testRust)
}
