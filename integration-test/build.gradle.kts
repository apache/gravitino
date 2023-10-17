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
  testImplementation(libs.iceberg.spark.runtime)
  testImplementation(libs.spark.sql) {
    exclude("org.apache.hadoop")
    exclude("org.rocksdb")
    exclude("org.apache.avro")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
  }
  testImplementation(libs.slf4j.jdk14)
}

/* Optimizing integration test execution conditions */
// Use this variable to control if we need to run docker test or not.
var HIVE_IMAGE_NAME = "datastrato/gravitino-ci-hive"
var HIVE_IMAGE_TAG_NAME = "${HIVE_IMAGE_NAME}:0.1.2"
var EXCLUDE_DOCKER_TEST = true
// Use these 3 variables allow for more detailed control in the future.
project.extra["dockerRunning"] = false
project.extra["hiveContainerRunning"] = false

fun printDockerCheckInfo() {
  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode != "deploy" && testMode != "embedded") {
    return
  }
  val dockerRunning = project.extra["dockerRunning"] as? Boolean ?: false
  val hiveContainerRunning = project.extra["hiveContainerRunning"] as? Boolean ?: false
  if (dockerRunning && hiveContainerRunning) {
    EXCLUDE_DOCKER_TEST = false
  }

  println("------------------ Check Docker environment -----------------")
  println("Docker server status ........................................ [${if (dockerRunning) "running" else "stop"}]")
  println("Gravitino IT Docker container is already running ............. [${if (hiveContainerRunning) "yes" else "no"}]")

  if (dockerRunning && hiveContainerRunning) {
    println("Use Gravitino IT Docker container to run all integration test. [$testMode test]")
  } else {
    println("Run test cases without `gravitino-docker-it` tag ............. [$testMode test]")
  }
  println("-------------------------------------------------------------")
}

tasks {
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
    dependsOn("checkContainerRunning")

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
      // Default use MiniGravitino to run integration tests
      environment("GRAVITINO_ROOT_DIR", rootDir.path)
      environment("HADOOP_USER_NAME", "hive")
      environment("HADOOP_HOME", "/tmp")
      environment("PROJECT_VERSION", version)

      val testMode = project.properties["testMode"] as? String ?: "embedded"
      systemProperty("gravitino.log.path", buildDir.path)
      if (testMode == "deploy") {
        environment("GRAVITINO_HOME", rootDir.path + "/distribution/package")
        systemProperty("testMode", "deploy")
      } else if (testMode == "embedded") {
        environment("GRAVITINO_HOME", rootDir.path)
        environment("GRAVITINO_TEST", "true")
        systemProperty("testMode", "embedded")
      } else {
        throw GradleException("Gravitino integration test only support [-PtestMode=embedded] or [-PtestMode=deploy] mode!")
      }

      useJUnitPlatform {
        if (EXCLUDE_DOCKER_TEST) {
          excludeTags("gravitino-docker-it")
        }
      }
    }
  }
}
