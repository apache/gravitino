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

fun gravitinoServer(operation: String) {
    val process = ProcessBuilder("${project.rootDir.path}/distribution/package/bin/gravitino.sh", operation).start()
    val exitCode = process.waitFor()
    if (exitCode == 0) {
      val currentContext = process.inputStream.bufferedReader().readText()
      println("Gravitino server status: $currentContext")
    } else {
      println("Gravitino server execution failed with exit code $exitCode")
    }
}

tasks {
  val pipInstall by registering(VenvTask::class) {
    venvExec = "pip"
    args = listOf("install", "-e", ".[dev]")
  }

  val black by registering(VenvTask::class) {
    venvExec = "black"
    args = listOf("./gravitino", "./tests")
  }

  val pylint by registering(VenvTask::class) {
    mustRunAfter(black)
    venvExec = "pylint"
    args = listOf("./gravitino", "./tests")
  }

  val test by registering(VenvTask::class) {
    val skipPyClientITs = project.hasProperty("skipPyClientITs")
    if (!skipPyClientITs) {
      doFirst {
        gravitinoServer("start")
      }

      dependsOn(pipInstall, black, pylint)
      venvExec = "python"
      args = listOf("-m", "unittest")
      workingDir = projectDir.resolve(".")
      environment = mapOf(
        "PROJECT_VERSION" to project.version,
        "GRAVITINO_HOME" to project.rootDir.path + "/distribution/package",
        "START_EXTERNAL_GRAVITINO" to "true"
      )

      doLast {
        gravitinoServer("stop")
      }
    }
  }

  val build by registering(VenvTask::class) {
    dependsOn(pipInstall, black, pylint)
  }

  val clean by registering(Delete::class) {
    delete("build")
    delete("gravitino.egg-info")

    doLast {
      deleteCacheDir(".pytest_cache")
      deleteCacheDir("__pycache__")
    }
  }
}
