/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
import org.gradle.internal.os.OperatingSystem
import java.io.IOException
import java.util.*

plugins {
  `maven-publish`
  `application`
  id("java")
  id("idea")
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark.get()
val icebergVersion: String = libs.versions.iceberg.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()

dependencies {
  implementation(project(":api"))
  implementation(project(":catalogs:catalog-hive"))
  implementation(project(":catalogs:catalog-jdbc-common"))
  implementation(project(":catalogs:catalog-jdbc-mysql"))
  implementation(project(":catalogs:catalog-jdbc-postgresql"))
  implementation(project(":catalogs:catalog-lakehouse-iceberg"))
  implementation(project(":catalogs:catalog-hadoop"))
  implementation(project(":clients:client-java"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":server"))
  implementation(project(":server-common"))
  implementation(project(":spark-connector"))
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.jwt)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.cli)
  implementation(libs.commons.io)
  implementation(libs.guava)
  implementation(libs.httpclient5)

  testImplementation(libs.hive2.metastore) {
    exclude("co.cask.tephra")
    exclude("com.github.joshelser")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.code.findbugs", "sr305")
    exclude("com.tdunning", "json")
    exclude("com.zaxxer", "HikariCP")
    exclude("io.dropwizard.metricss")
    exclude("javax.transaction", "transaction-api")
    exclude("org.apache.avro")
    exclude("org.apache.curator")
    exclude("org.apache.hbase")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
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

  testImplementation(libs.hadoop2.common) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.hdfs) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  testImplementation(libs.hive2.common) {
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
  }

  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.guava)
  testImplementation(libs.httpclient5)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation("org.apache.iceberg:iceberg-spark-runtime-3.4_$scalaVersion:$icebergVersion")
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion")
  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }
  testImplementation(libs.jline.terminal)
  testImplementation(libs.minikdc) {
    exclude("org.apache.directory.api", "api-ldap-schema-data")
  }
  testImplementation(libs.mysql.driver)
  testImplementation(libs.okhttp3.loginterceptor)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.rauschig)
  testImplementation(libs.selenium)
  testImplementation(libs.slf4j.jdk14)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.trino.cli)
  testImplementation(libs.trino.client) {
    exclude("jakarta.annotation")
  }
  testImplementation(libs.trino.jdbc)
  testRuntimeOnly(libs.junit.jupiter.engine)
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
        " the integration test on macOS."
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
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    doFirst {
      // Get current project version
      val version = project.version.toString()
      println("Current project version: $version")

      // Check whether this module has already built
      val trinoConnectorBuildDir = project(":trino-connector").buildDir
      if (trinoConnectorBuildDir.exists()) {
        // Check the version gravitino related jars in build equal to the current project version
        val invalidGravitinoJars = trinoConnectorBuildDir.resolve("libs").listFiles { _, name -> name.startsWith("gravitino") }?.filter {
          val name = it.name
          !name.endsWith(version + ".jar")
        }

        if (invalidGravitinoJars!!.isNotEmpty()) {
          val message = "Found mismatched versions of gravitino jars in trino-connector/build/libs:\n" +
            "${invalidGravitinoJars.joinToString(", ") { it.name }}\n" +
            "The current version of the project is $version. Please clean the project and rebuild it."
          throw GradleException(message)
        }
      }

      printDockerCheckInfo()

      copy {
        from("${project.rootDir}/dev/docker/trino/conf")
        into("build/trino-conf")
        fileMode = 0b111101101
      }

      jvmArgs(project.property("extraJvmArgs") as List<*>)

      // Default use MiniGravitino to run integration tests
      environment("GRAVITINO_ROOT_DIR", rootDir.path)
      environment("IT_PROJECT_DIR", buildDir.path)
      environment("HADOOP_USER_NAME", "datastrato")
      environment("HADOOP_HOME", "/tmp")
      environment("PROJECT_VERSION", version)
      environment("TRINO_CONF_DIR", buildDir.path + "/trino-conf")

      val dockerRunning = project.extra["dockerRunning"] as? Boolean ?: false
      val macDockerConnector = project.extra["macDockerConnector"] as? Boolean ?: false
      if (OperatingSystem.current().isMacOsX() &&
        dockerRunning &&
        macDockerConnector
      ) {
        environment("NEED_CREATE_DOCKER_NETWORK", "true")
      }

      // Gravitino CI Docker image
      environment("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "datastrato/gravitino-ci-hive:0.1.8")
      environment("GRAVITINO_CI_TRINO_DOCKER_IMAGE", "datastrato/gravitino-ci-trino:0.1.5")

      // Change poll image pause time from 30s to 60s
      environment("TESTCONTAINERS_PULL_PAUSE_TIMEOUT", "60")

      val testMode = project.properties["testMode"] as? String ?: "embedded"
      systemProperty("gravitino.log.path", buildDir.path + "/integration-test.log")
      delete(buildDir.path + "/integration-test.log")
      if (testMode == "deploy") {
        environment("GRAVITINO_HOME", rootDir.path + "/distribution/package")
        systemProperty("testMode", "deploy")
      } else if (testMode == "embedded") {
        environment("GRAVITINO_HOME", rootDir.path)
        environment("GRAVITINO_TEST", "true")
        environment("GRAVITINO_WAR", rootDir.path + "/web/dist/")
        systemProperty("testMode", "embedded")
      } else {
        throw GradleException("Gravitino integration tests only support [-PtestMode=embedded] or [-PtestMode=deploy] mode!")
      }

      useJUnitPlatform {
        if (!DOCKER_IT_TEST) {
          excludeTags("gravitino-docker-it")
        }
      }
    }
  }
}

tasks.register<JavaExec>("TrinoTest") {
  classpath = sourceSets["test"].runtimeClasspath
  systemProperty("gravitino.log.path", buildDir.path + "/integration-test.log")
  mainClass.set("com.datastrato.gravitino.integration.test.trino.TrinoQueryTestTool")

  if (JavaVersion.current() > JavaVersion.VERSION_1_8) {
    jvmArgs = listOf(
      "--add-opens",
      "java.base/java.lang=ALL-UNNAMED"
    )
  }

  if (project.hasProperty("appArgs")) {
    args = (project.property("appArgs") as String).removeSurrounding("\"").split(" ")
  }
}
