import org.gradle.internal.os.OperatingSystem

/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-lakehouse-iceberg"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark.get()
val icebergVersion: String = libs.versions.iceberg.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":server-common"))
  implementation(libs.bundles.iceberg)
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.io)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.iceberg.hive.metastore)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.sqlite.jdbc)

  implementation(libs.hive2.metastore) {
    exclude("co.cask.tephra")
    exclude("com.github.spotbugs")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.tdunning", "json")
    exclude("javax.transaction", "transaction-api")
    exclude("org.apache.avro", "avro")
    exclude("org.apache.hbase")
    exclude("org.apache.hadoop", "hadoop-yarn-api")
    exclude("org.apache.hadoop", "hadoop-yarn-server-applicationhistoryservice")
    exclude("org.apache.hadoop", "hadoop-yarn-server-common")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.pentaho") // missing dependency
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("com.zaxxer", "HikariCP")
    exclude("com.sun.jersey", "jersey-server")
  }

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(project(":catalogs:catalog-jdbc-common", "testArtifacts"))
  testImplementation(project(":test-common", "testArtifacts"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  implementation(libs.hadoop2.common) {
    exclude("com.github.spotbugs")
  }
  implementation(libs.hadoop2.hdfs)
  implementation(libs.hadoop2.mapreduce.client.core)
  implementation(libs.metrics.jersey2)

  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")
  testImplementation("org.apache.iceberg:iceberg-spark-runtime-3.4_$scalaVersion:$icebergVersion")
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }

  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.testcontainers)

  // For test TestMultipleJDBCLoad
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)

  testImplementation(libs.slf4j.api)
  testImplementation(libs.bundles.log4j)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }

  val copyCatalogLibs by registering(Copy::class) {
    dependsOn("jar", "runtimeJars")
    from("build/libs")
    into("$rootDir/distribution/package/catalogs/lakehouse-iceberg/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/lakehouse-iceberg/conf")

    include("lakehouse-iceberg.conf")
    include("core-site.xml.template")
    include("hdfs-site.xml.template")

    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }

    exclude { details ->
      details.file.isDirectory()
    }
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyCatalogLibs, copyCatalogConfig)
  }
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/**")
  } else {
    dependsOn(tasks.jar)

    doFirst {
      jvmArgs(project.property("extraJvmArgs") as List<*>)

      // Default use MiniGravitino to run integration tests
      environment("GRAVITINO_ROOT_DIR", rootDir.path)
      environment("IT_PROJECT_DIR", buildDir.path)
      environment("HADOOP_USER_NAME", "datastrato")
      environment("HADOOP_HOME", "/tmp")
      environment("PROJECT_VERSION", version)

      val dockerRunning = project.rootProject.extra["dockerRunning"] as? Boolean ?: false
      val macDockerConnector = project.rootProject.extra["macDockerConnector"] as? Boolean ?: false
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
      systemProperty("gravitino.log.path", buildDir.path + "/${project.name}-integration-test.log")
      delete(buildDir.path + "/${project.name}-integration-test.log")
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
        val DOCKER_IT_TEST = project.rootProject.extra["docker_it_test"] as? Boolean ?: false
        if (!DOCKER_IT_TEST) {
          excludeTags("gravitino-docker-it")
        }
      }
    }
  }
}
