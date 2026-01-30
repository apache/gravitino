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

tasks.named<War>("war") {
  enabled = false
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

  // Separate webpack task for legacy frontend to produce the "v1" dist.
  val webpackV1 by registering(PnpmTask::class) {
    dependsOn(lintCheck, prettierCheck)
    args = listOf("dist:v1")
    environment.put("NODE_ENV", "production")
    doLast {
      val srcDir = file("dist")
      val dstDir = file("dist-v1")
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
    dependsOn(lintCheck, prettierCheck, webpackV1)
    // Allow overriding the frontend dist task via Gradle project property:
    // -PfrontendDist=dist:v1  -> run `pnpm run dist:v1`
    // -PfrontendDist=dist      -> run `pnpm run dist` (default)
    val frontendDistArg: String = if (project.hasProperty("frontendDist")) {
      project.property("frontendDist").toString()
    } else {
      "dist"
    }
    args = listOf(frontendDistArg)
    environment.put("NODE_ENV", "production")
  }

  // War task for legacy frontend (produces classifier '-v1')
  val buildWarV1 by registering(War::class) {
    dependsOn(webpackV1)
    archiveBaseName.set("${rootProject.name}-web")
    archiveClassifier.set("v1")
    archiveVersion.set("")
    from("./WEB-INF") {
      into("WEB-INF")
    }
    from("dist-v1") {
      into("")
    }
  }

  // War task for default (new) frontend
  val buildWarV2 by registering(War::class) {
    dependsOn(webpack)
    archiveBaseName.set("${rootProject.name}-web-v2")
    // no classifier for the new WAR
    from("./WEB-INF") {
      into("WEB-INF")
    }
    from("dist") {
      into("")
    }
  }

  // Configure the `build` lifecycle to produce either only the legacy WAR
  // (when caller set -PfrontendDist=dist:v1) or both legacy+new by default.
  build {
    if (project.hasProperty("frontendDist") && project.property("frontendDist").toString() == "dist:v1") {
      dependsOn(buildWarV1)
    } else {
      dependsOn(buildWarV2, buildWarV1)
    }
  }

  clean {
    delete(".node")
    delete("build")
    delete(".next")
    delete("dist")
    delete("dist-v1")
    delete("node_modules")
    delete("yarn-error.log")
  }
}
