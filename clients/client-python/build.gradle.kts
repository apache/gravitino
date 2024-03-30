/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
import io.github.piyushroshan.python.VenvTask

plugins {
  id("io.github.piyushroshan.python-gradle-miniforge-plugin") version "1.0.0"
}

pythonPlugin {
  pythonVersion.set(project.rootProject.extra["pythonVersion"].toString())
}

tasks {
  register<VenvTask>("condaInfo") {
    venvExec = "conda"
    args = listOf("info")
  }

  val pipInstall by registering(VenvTask::class) {
    venvExec = "pip"
    args = listOf("install", "--isolated", "-r", "requirements.txt")
  }

  val runPyTests by registering(VenvTask::class) {
    dependsOn(pipInstall)
    venvExec = "pytest"
    workingDir = projectDir.resolve("tests")
  }

  compileJava {
    dependsOn(pipInstall)
  }

  test {
    dependsOn(runPyTests)
    useJUnitPlatform()
    testLogging.showStandardStreams = true
  }

  clean {
    delete("build")
  }
}
