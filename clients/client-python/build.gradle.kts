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

fun deleteCacheDir(targetDir: String) {
  project.fileTree(project.projectDir).matching {
    include("**/$targetDir/**")
  }.forEach { file ->
    val targetDirPath = file.path.substring(0, file.path.lastIndexOf(targetDir) + targetDir.length)
    project.file(targetDirPath).deleteRecursively()
  }
}

tasks {
  register<VenvTask>("condaInfo") {
    venvExec = "conda"
    args = listOf("info")
  }

  val pipInstall by registering(VenvTask::class) {
    venvExec = "pip"
    args = listOf("install", "-e", ".")
  }

  val runPyTests by registering(VenvTask::class) {
    dependsOn(pipInstall)
    venvExec = "python"
    args = listOf("-m", "unittest")
    workingDir = projectDir.resolve(".")
  }

  // https://github.com/datastrato/gravitino/issues/2770
  // Improved Gradle Build Scripts for Python Module
  compileTestJava {
    dependsOn(runPyTests)
  }

  compileJava {
    dependsOn(runPyTests)
  }

  test {
    val skipPythonITs = project.hasProperty("skipPythonITs")
    if (!skipPythonITs) {
      dependsOn(runPyTests)
    }
  }

  clean {
    delete("build")
    delete("gravitino.egg-info")

    doLast {
      deleteCacheDir(".pytest_cache")
      deleteCacheDir("__pycache__")
    }
  }
}
