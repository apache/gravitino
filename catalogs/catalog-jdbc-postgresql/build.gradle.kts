import org.gradle.internal.os.OperatingSystem
import java.io.IOException
import java.util.*

/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-jdbc-postgresql"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {

  implementation(project(":api"))
  implementation(project(":catalogs:catalog-jdbc-common"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jsqlparser)

  testImplementation(project(":catalogs:catalog-jdbc-common", "testArtifacts"))
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.guava)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs_all")
  }
  val copyCatalogLibs by registering(Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs_all", "build/libs")
    into("$rootDir/distribution/package/catalogs/jdbc-postgresql/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/jdbc-postgresql/conf")

    include("jdbc-postgresql.conf")

    exclude { details ->
      details.file.isDirectory()
    }
  }

  val copyLibAndConfig by registering(Copy::class) {
    dependsOn(copyCatalogLibs, copyCatalogConfig)
  }
}

/* Optimizing integration test execution conditions */
// Use this variable to control if we need to run docker test or not.
var DOCKER_IT_TEST = false
project.extra["dockerRunning"] = false
project.extra["macDockerConnector"] = false
project.extra["isOrbStack"] = false

fun printDockerCheckInfo() {
  checkMacDockerConnector()
  checkDockerStatus()
  checkOrbStackStatus()

  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode != "deploy" && testMode != "embedded") {
    return
  }
  val dockerRunning = project.extra["dockerRunning"] as? Boolean ?: false
  val macDockerConnector = project.extra["macDockerConnector"] as? Boolean ?: false
  val isOrbStack = project.extra["isOrbStack"] as? Boolean ?: false

  if (OperatingSystem.current().isMacOsX() &&
    dockerRunning &&
    (macDockerConnector || isOrbStack)
  ) {
    DOCKER_IT_TEST = true
  } else if (OperatingSystem.current().isLinux() && dockerRunning) {
    DOCKER_IT_TEST = true
  }

  println("------------------ Check Docker environment ---------------------")
  println("Docker server status ............................................ [${if (dockerRunning) "running" else "stop"}]")
  if (OperatingSystem.current().isMacOsX()) {
    println("mac-docker-connector status ..................................... [${if (macDockerConnector) "running" else "stop"}]")
    println("OrbStack status ................................................. [${if (isOrbStack) "yes" else "no"}]")
  }
  if (!DOCKER_IT_TEST) {
    println("Run test cases without `gravitino-docker-it` tag ................ [$testMode test]")
  } else {
    println("Using Gravitino IT Docker container to run all integration tests. [$testMode test]")
  }
  println("-----------------------------------------------------------------")

  // Print help message if Docker server or mac-docker-connector is not running
  printDockerServerTip()
  printMacDockerTip()
}

fun printDockerServerTip() {
  val dockerRunning = project.extra["dockerRunning"] as? Boolean ?: false
  if (!dockerRunning) {
    val redColor = "\u001B[31m"
    val resetColor = "\u001B[0m"
    println("Tip: Please make sure to start the ${redColor}Docker server$resetColor before running the integration tests.")
  }
}

fun printMacDockerTip() {
  val macDockerConnector = project.extra["macDockerConnector"] as? Boolean ?: false
  val isOrbStack = project.extra["isOrbStack"] as? Boolean ?: false
  if (OperatingSystem.current().isMacOsX() && !macDockerConnector && !isOrbStack) {
    val redColor = "\u001B[31m"
    val resetColor = "\u001B[0m"
    println(
      "Tip: Please make sure to use ${redColor}OrbStack$resetColor or execute the " +
        "$redColor`dev/docker/tools/mac-docker-connector.sh`$resetColor script before running" +
        " the integration test or unit test that depends on docker environment on macOS."
    )
  }
}

fun checkMacDockerConnector() {
  if (OperatingSystem.current().isLinux()) {
    // Linux does not require the use of `docker-connector`
    return
  }

  try {
    val processName = "docker-connector"
    val command = "pgrep -x -q $processName"

    val execResult = project.exec {
      commandLine("bash", "-c", command)
    }
    if (execResult.exitValue == 0) {
      project.extra["macDockerConnector"] = true
    }
  } catch (e: Exception) {
    println("checkContainerRunning command execution failed: ${e.message}")
  }
}

fun checkDockerStatus() {
  try {
    val process = ProcessBuilder("docker", "info").start()
    val exitCode = process.waitFor()

    if (exitCode == 0) {
      project.extra["dockerRunning"] = true
    } else {
      println("checkDockerStatus command execution failed with exit code $exitCode")
    }
  } catch (e: IOException) {
    println("checkDockerStatus command execution failed: ${e.message}")
  }
}

fun checkOrbStackStatus() {
  if (OperatingSystem.current().isLinux()) {
    return
  }

  try {
    val process = ProcessBuilder("docker", "context", "show").start()
    val exitCode = process.waitFor()
    if (exitCode == 0) {
      val currentContext = process.inputStream.bufferedReader().readText()
      println("Current docker context is: $currentContext")
      project.extra["isOrbStack"] = currentContext.lowercase(Locale.getDefault()).contains("orbstack")
    } else {
      println("checkOrbStackStatus Command execution failed with exit code $exitCode")
    }
  } catch (e: IOException) {
    println("checkOrbStackStatus command execution failed: ${e.message}")
  }
}

tasks.test {
  doFirst {
    printDockerCheckInfo()
    useJUnitPlatform {
      if (!DOCKER_IT_TEST) {
        excludeTags("gravitino-docker-it")
      }
    }
  }
}
