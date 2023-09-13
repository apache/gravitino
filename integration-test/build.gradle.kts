/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import java.io.IOException
import kotlin.io.*

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
}

dependencies {
  implementation(project(":server"))
  implementation(project(":common"))
  implementation(project(":client-java"))
  implementation(project(":api"))
  implementation(project(":core"))
  implementation(project(":catalog-hive"))
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.jetty)
  implementation(libs.httpclient5)
  implementation(libs.commons.io)

  testImplementation(libs.hive2.metastore) {
    exclude("org.apache.hbase")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("co.cask.tephra")
    exclude("org.apache.avro")
    exclude("org.apache.zookeeper")
    exclude("org.apache.logging.log4j")
    exclude("com.google.code.findbugs", "sr305")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("com.tdunning", "json")
    exclude("javax.transaction", "transaction-api")
    exclude("com.zaxxer", "HikariCP")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("org.apache.curator")
    exclude("com.github.joshelser")
    exclude("io.dropwizard.metricss")
    exclude("org.slf4j")
  }

  testImplementation(libs.hive2.exec) {
    artifact {
      classifier = "core"
    }
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.avro")
    exclude("org.apache.zookeeper")
    exclude("com.google.protobuf")
    exclude("org.apache.calcite")
    exclude("org.apache.calcite.avatica")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.curator")
    exclude("org.pentaho")
    exclude("org.slf4j")
  }

  testImplementation(libs.hive2.common) {
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
  }

  testImplementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.common) {
    exclude("*")
  }

  testImplementation(libs.substrait.java.core) {
    exclude("org.slf4j")
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
  }

  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
  testImplementation(libs.guava)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.httpclient5)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.mockito.core)
  testImplementation(libs.bundles.log4j)
}

var HIVE_IMAGE_NAME = "datastrato/graviton-ci-hive:0.1.1"
var EXCLUDE_DOCKER_TEST = true
// Use these 3 variables allow for more detailed control in the future.
project.extra["dockerRunning"] = false
project.extra["hiveImageExists"] = false
project.extra["hiveContainerRunning"] = false

fun printDockerCheckInfo() {
  val dockerRunning = project.extra["dockerRunning"] as? Boolean ?: false
  val hiveImageExists = project.extra["hiveImageExists"] as? Boolean ?: false
  val hiveContainerRunning = project.extra["hiveContainerRunning"] as? Boolean ?: false
  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (dockerRunning && hiveImageExists && hiveContainerRunning) {
    EXCLUDE_DOCKER_TEST = false;
  }

  println("-------------------- Check Docker environment --------------------")
  println("Docker server status ............................................. [${if (dockerRunning) "Running" else "Stop"}]")
  println("Graviton CI Hive Image exists locally ............................ [${if (hiveImageExists) "Yes" else "No"}]")
  println("Graviton CI Hive container is already running .................... [${if (hiveContainerRunning) "Yes" else "No"}]")

  if (dockerRunning && hiveImageExists && hiveContainerRunning) {
    println("Use exist Graviton CI Hive container to run all integration test.  [$testMode Test]")
  } else {
    println("Run only test cases where tag is not set special `DOCKER-NAME`.    [$testMode Test]")
  }
  println("------------------------------------------------------------------")
}

tasks {
  // Use this task to check if docker container is running
  register("checkContainerRunning") {
    doLast {
      try {
        val process = ProcessBuilder("docker", "ps", "--filter", "ancestor=$HIVE_IMAGE_NAME", "--format", "{{.ID}}").start()
        val exitCode = process.waitFor()

        if (exitCode == 0) {
          val output = process.inputStream.bufferedReader().readText()
          if (output.isNotEmpty()) {
            project.extra["hiveContainerRunning"] = true
          }
        } else {
          println("checkContainerRunning command execution failed with exit code $exitCode")
        }
      } catch (e: IOException) {
        println("checkContainerRunning command execution failed: ${e.message}")
      }
    }
  }

  // Use this task to check if docker image exists
  register("checkDockerImageExists") {
    dependsOn("checkContainerRunning")
    doLast {
      try {
        val process = ProcessBuilder("docker", "inspect", "--format='{{.Config.Cmd}}'", "$HIVE_IMAGE_NAME").start()
        val exitCode = process.waitFor()

        if (exitCode == 0) {
          project.extra["hiveImageExists"] = true
        } else {
          println("checkDockerImageExists command execution failed with exit code $exitCode")
        }
      } catch (e: IOException) {
        println("checkDockerImageExists command execution failed: ${e.message}")
      }
    }
  }

  // Use this task to check if docker is running
  register("checkDockerRunning") {
    dependsOn("checkDockerImageExists")

    doLast {
      try {
        val process = ProcessBuilder("docker", "info").start()
        val exitCode = process.waitFor()

        if (exitCode == 0) {
          project.extra["dockerRunning"] = true
        } else {
          println("checkDockerRunning Command execution failed with exit code $exitCode")
        }
      } catch (e: IOException) {
        println("checkDockerRunning command execution failed: ${e.message}")
      }
      printDockerCheckInfo()
    }
  }

  register("runHiveContainer") {
    doLast {
      try {
        val process = ProcessBuilder("docker", "run", "--rm", "-d", "-p", "8088:8088",
          "-p", "50070:50070", "-p", "50075:50075", "-p", "10000:10000", "-p", "10002:10002",
          "-p", "8888:8888", "-p", "9083:9083", "-p", "8022:22", "$HIVE_IMAGE_NAME").start()
        val exitCode = process.waitFor()

        if (exitCode == 0) {
          println("runHiveContainer Command execution successful")
        } else {
          println("runHiveContainer command execution failed with exit code $exitCode")
        }
      } catch (e: IOException) {
        println("runHiveContainer command execution failed: ${e.message}")
      }
    }
  }
}

tasks.test {
  dependsOn("checkDockerRunning")

  // Default use MiniGraviton to run integration tests
  environment("GRAVITON_ROOT_DIR", rootDir.path)
  environment("HADOOP_USER_NAME", "hive")
  environment("HADOOP_HOME", "/tmp")
  environment("PROJECT_VERSION", version)

  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode == "deploy") {
    systemProperty("TestMode", "deploy")
  } else {
    // default use embedded mode
    systemProperty("TestMode", "embedded")
  }

  useJUnitPlatform{
    if (EXCLUDE_DOCKER_TEST) {
      excludeTags("graviton-ci-hive")
    }

    if (testMode == "embedded") {
        excludeTags("deploy") // exclude all test cases with tag `deploy` mode
    }
  }
}
