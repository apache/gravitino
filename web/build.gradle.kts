/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import com.github.gradle.node.yarn.task.YarnTask

plugins {
  id("war")
}

tasks.withType(YarnTask::class) {
  workingDir.set(file("${project.projectDir}"))
}

tasks {
  // Install dependencies
  val yarnInstall by registering(YarnTask::class) {
    args = listOf("install")
  }

  // Check for lint errors
  val lintCheck by registering(YarnTask::class) {
    dependsOn(yarnInstall)
    args = listOf("lint")
  }

  // Check for prettier errors
  val prettierCheck by registering(YarnTask::class) {
    dependsOn(yarnInstall)
    args = listOf("prettier:check")
  }

  val webpack by registering(YarnTask::class) {
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
    delete("build")
    delete("dist")
  }
}
