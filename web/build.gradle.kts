/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
    delete("dist")
    delete("node_modules")
    delete("yarn-error.log")
  }
}
