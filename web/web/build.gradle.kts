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

  // Separate webpack task for legacy frontend to produce the "old" dist.
  val webpackOld by registering(PnpmTask::class) {
    dependsOn(lintCheck, prettierCheck)
    args = listOf("dist:old")
    environment.put("NODE_ENV", "production")
    doLast {
      val srcDir = file("dist")
      val dstDir = file("dist-old")
      if (dstDir.exists()) {
        dstDir.deleteRecursively()
      }
      if (srcDir.exists()) {
        copy {
          from(srcDir)
          into(dstDir)
        }
      }
    }
  }

  val webpack by registering(PnpmTask::class) {
    dependsOn(lintCheck, prettierCheck, webpackOld)
    // Allow overriding the frontend dist task via Gradle project property:
    // -PfrontendDist=dist:old  -> run `pnpm run dist:old`
    // -PfrontendDist=dist      -> run `pnpm run dist` (default)
    val frontendDistArg: String = if (project.hasProperty("frontendDist")) {
      project.property("frontendDist").toString()
    } else {
      "dist"
    }
    args = listOf(frontendDistArg)
    environment.put("NODE_ENV", "production")
  }

  // War task for legacy frontend (produces classifier '-old')
  val buildWarOld by registering(War::class) {
    dependsOn(webpackOld)
    archiveClassifier.set("old")
    from("./WEB-INF") {
      into("WEB-INF")
    }
    from("dist-old") {
      into("")
    }
  }

  // War task for default (new) frontend
  val buildWarNew by registering(War::class) {
    dependsOn(webpack)
    // no classifier for the new WAR
    from("./WEB-INF") {
      into("WEB-INF")
    }
    from("dist") {
      into("")
    }
  }

  // Configure the `build` lifecycle to produce either only the legacy WAR
  // (when caller set -PfrontendDist=dist:old) or both legacy+new by default.
  build {
    if (project.hasProperty("frontendDist") && project.property("frontendDist").toString() == "dist:old") {
      dependsOn(buildWarOld)
    } else {
      dependsOn(buildWarNew, buildWarOld)
    }
  }

  clean {
    delete(".node")
    delete("build")
    delete(".next")
    delete("dist")
    delete("dist-old")
    delete("node_modules")
    delete("yarn-error.log")
  }
}
