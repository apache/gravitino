/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import com.github.gradle.node.yarn.task.YarnTask

plugins {
  id("war")
  id("com.github.node-gradle.node") version "3.1.0"
}

node {
  version = "20.9.0"
  npmVersion = "10.1.0"
  yarnVersion = "1.22.19"
  workDir = file("${project.projectDir}/.node")
  download = true
}

tasks {
  val buildwar by registering(War::class) {
    dependsOn("webpack")
    from("src/WEB-INF") {
      into("WEB-INF")
    }
    from("dist") {
      into("")
    }
  }

  // Install dependencies
  val yarnInstall by registering(YarnTask::class) {
    args = listOf("install")
  }

  // Check for lint errors
  val lintCheck by registering(YarnTask::class) {
    dependsOn("yarnInstall")
    args = listOf("lint")
  }

  // Check for prettier errors
  val prettierCheck by registering(YarnTask::class) {
    dependsOn("yarnInstall")
    args = listOf("prettier:check")
  }

  val webpack by registering(YarnTask::class) {
    dependsOn("lintCheck", "prettierCheck")
    args = listOf("dist")
    environment.put("NODE_ENV", "production")
  }

  build {
    dependsOn(buildwar)
  }
  clean {
    delete(".gradle")
    delete(".next")
    delete(".node")
    delete("build")
    delete("dist")
    delete("node_modules")
  }
}
