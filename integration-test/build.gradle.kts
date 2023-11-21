/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import java.io.IOException
import kotlin.io.*
import org.gradle.internal.os.OperatingSystem

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
}

dependencies {
  implementation(project(":server"))
  implementation(project(":common"))
  implementation(project(":server-common"))
  implementation(project(":clients:client-java"))
  implementation(project(":api"))
  implementation(project(":core"))
  implementation(project(":catalogs:catalog-hive"))
  implementation(project(":catalogs:catalog-lakehouse-iceberg"))
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jwt)
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
  testImplementation(libs.hadoop2.hdfs) {
    exclude("*")
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
  testImplementation(libs.iceberg.spark.runtime)
  testImplementation(libs.spark.sql) {
    exclude("org.apache.hadoop")
    exclude("org.rocksdb")
    exclude("org.apache.avro")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
  }
  testImplementation(libs.slf4j.jdk14)
  testImplementation(libs.scala.collection.compat)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.spark.hive)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.trino.jdbc)
}

/* Optimizing integration test execution conditions */
// Use this variable to control if we need to run docker test or not.
var HIVE_IMAGE_NAME = "datastrato/gravitino-ci-hive"
var HIVE_IMAGE_TAG_NAME = "${HIVE_IMAGE_NAME}:0.1.2"
var EXCLUDE_DOCKER_TEST = true
var EXCLUDE_TRINO_TEST = true
// Use these 3 variables allow for more detailed control in the future.
project.extra["dockerRunning"] = false
project.extra["hiveContainerRunning"] = false
project.extra["macDockerConnector"] = false

fun printDockerCheckInfo() {
  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode != "deploy" && testMode != "embedded") {
    return
  }
  val dockerRunning = project.extra["dockerRunning"] as? Boolean ?: false
  val hiveContainerRunning = project.extra["hiveContainerRunning"] as? Boolean ?: false
  val macDockerConnector = project.extra["macDockerConnector"] as? Boolean ?: false
  if (dockerRunning && hiveContainerRunning) {
    EXCLUDE_DOCKER_TEST = false
  }
  if (dockerRunning && macDockerConnector) {
    EXCLUDE_TRINO_TEST = false
  }

  println("------------------ Check Docker environment ---------------------")
  println("Docker server status ............................................ [${if (dockerRunning) "running" else "stop"}]")
  println("Gravitino IT Docker container is already running ................ [${if (hiveContainerRunning) "yes" else "no"}]")
  if (OperatingSystem.current().isMacOsX() && !(dockerRunning && macDockerConnector)) {
    println("Run test cases without `gravitino-trino-it` tag ................. [$testMode test]")
  }
  if (dockerRunning && hiveContainerRunning) {
    println("Using Gravitino IT Docker container to run all integration tests. [$testMode test]")
  } else {
    println("Run test cases without `gravitino-docker-it` tag ................ [$testMode test]")
  }
  println("-----------------------------------------------------------------")
}

tasks {
  register("isMacDockerConnectorRunning") {
    doLast {
      val processName = "docker-connector"
      val command = "pgrep -x ${processName}"

      try {
        val execResult = project.exec {
          commandLine("bash", "-c", command)
        }
        if (execResult.exitValue == 0) {
          project.extra["macDockerConnector"] = true
        } else {
          project.extra["macDockerConnector"] = false
        }
      } catch (e: Exception) {
        project.extra["macDockerConnector"] = false
      }
    }
  }

  // Use this task to check if docker container is running
  register("checkContainerRunning") {
    doLast {
      try {
        val process = ProcessBuilder("docker", "ps", "--format='{{.Image}}'").start()
        val exitCode = process.waitFor()

        if (exitCode == 0) {
          val output = process.inputStream.bufferedReader().readText()
          val haveHiveContainer = output.contains("${HIVE_IMAGE_NAME}")
          if (haveHiveContainer) {
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

  // Use this task to check if docker is running
  register("checkDockerRunning") {
    dependsOn("checkContainerRunning", "isMacDockerConnectorRunning")

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
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    dependsOn("checkDockerRunning")

    doFirst {
      copy {
        from("${project.rootDir}/dev/docker/trino/conf")
        into("build/trino-conf")
        fileMode = 0b111101101
      }

      // Default use MiniGravitino to run integration tests
      environment("GRAVITINO_ROOT_DIR", rootDir.path)
      // TODO: use hive user instead after we fix the permission issue #554
      environment("HADOOP_USER_NAME", "root")
      environment("HADOOP_HOME", "/tmp")
      environment("PROJECT_VERSION", version)
      environment("TRINO_CONF_DIR", buildDir.path + "/trino-conf")

      // Gravitino CI Docker image
      environment("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "datastrato/gravitino-ci-hive:0.1.5")
      environment("GRAVITINO_CI_TRINO_DOCKER_IMAGE", "datastrato/gravitino-ci-trino:0.1.0")

      val testMode = project.properties["testMode"] as? String ?: "embedded"
      systemProperty("gravitino.log.path", buildDir.path + "/integration-test.log")
      delete(buildDir.path + "/integration-test.log")
      if (testMode == "deploy") {
        environment("GRAVITINO_HOME", rootDir.path + "/distribution/package")
        systemProperty("testMode", "deploy")
      } else if (testMode == "embedded") {
        environment("GRAVITINO_HOME", rootDir.path)
        environment("GRAVITINO_TEST", "true")
        systemProperty("testMode", "embedded")
      } else {
        throw GradleException("Gravitino integration tests only support [-PtestMode=embedded] or [-PtestMode=deploy] mode!")
      }

      useJUnitPlatform {
        if (EXCLUDE_DOCKER_TEST) {
          val redColor = "\u001B[31m"
          val resetColor = "\u001B[0m"
          println("${redColor}Gravitino-docker is not running locally, all integration test cases tagged with 'gravitino-docker-it' will be excluded.${resetColor}")
          excludeTags("gravitino-docker-it")
        }
        if (EXCLUDE_TRINO_TEST && OperatingSystem.current().isMacOsX()) {
          val redColor = "\u001B[31m"
          val resetColor = "\u001B[0m"
          println("${redColor}mac-docker-connector is not running locally, all integration test cases tagged with 'gravitino-trino-it' will be excluded.${resetColor}")
          excludeTags("gravitino-trino-it")
        }
      }
    }
  }
}
