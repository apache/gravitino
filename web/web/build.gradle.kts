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

import com.github.gradle.node.pnpm.task.PnpmTask

plugins {
  id("war")
}

tasks.withType(PnpmTask::class) {
  workingDir.set(file("${project.projectDir}"))
}

tasks {
  // Install dependencies
  val installDeps by registering(PnpmTask::class) {
    args = listOf("install")
  }

  // Check for lint errors
  val lintCheck by registering(PnpmTask::class) {
    dependsOn(installDeps)
    args = listOf("lint")
  }

  // Check for prettier errors
  val prettierCheck by registering(PnpmTask::class) {
    dependsOn(installDeps)
    args = listOf("prettier:check")
  }

  val webpack by registering(PnpmTask::class) {
    dependsOn(lintCheck, prettierCheck)
    args = listOf("dist")
    environment.put("NODE_ENV", "production")
  }

  val buildWar by registering(War::class) {
    dependsOn(webpack)
    from("./WEB-INF") {
      into("WEB-INF")
    }
    from("dist") {
      into("")
    }
  }

  build {
    dependsOn(buildWar)
  }

  clean {
    delete(".node")
    delete("build")
    delete(".next")
    delete("dist")
    delete("node_modules")
    delete("yarn-error.log")
  }
}
