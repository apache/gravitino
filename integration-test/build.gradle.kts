/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
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
  testAnnotationProcessor(libs.lombok)

  testCompileOnly(libs.lombok)

  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":clients:filesystem-hadoop3"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(project(":spark-connector:spark-connector")) {
    exclude("org.apache.hadoop", "hadoop-client-api")
    exclude("org.apache.hadoop", "hadoop-client-runtime")
  }

  testImplementation(libs.commons.cli)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.guava)
  testImplementation(libs.commons.io)
  testImplementation(libs.bundles.jetty)
  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.hadoop2.common) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.hdfs)
  testImplementation(libs.hive2.common) {
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
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
  testImplementation(libs.httpclient5)
  testImplementation(libs.jline.terminal)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.minikdc) {
    exclude("org.apache.directory.api", "api-ldap-schema-data")
  }
  testImplementation(libs.mockito.core)
  testImplementation(libs.mysql.driver)

  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion") {
    exclude("org.apache.hadoop", "hadoop-client-api")
    exclude("org.apache.hadoop", "hadoop-client-runtime")
  }
  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }

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

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    val skipWebITs = project.hasProperty("skipWebITs")
    if (skipWebITs) {
      exclude("**/integration/test/web/ui/**")
    }

    dependsOn(":trino-connector:jar")
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar", ":catalogs:catalog-lakehouse-iceberg:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-doris:jar", ":catalogs:catalog-jdbc-doris:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-mysql:jar", ":catalogs:catalog-jdbc-mysql:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-postgresql:jar", ":catalogs:catalog-jdbc-postgresql:runtimeJars")
    dependsOn(":catalogs:catalog-hadoop:jar", ":catalogs:catalog-hadoop:runtimeJars")
    dependsOn(":catalogs:catalog-hive:jar", ":catalogs:catalog-hive:runtimeJars")

    doFirst {
      // Gravitino CI Docker image
      environment("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "datastrato/gravitino-ci-hive:0.1.10")
      environment("GRAVITINO_CI_TRINO_DOCKER_IMAGE", "datastrato/gravitino-ci-trino:0.1.5")

      copy {
        from("${project.rootDir}/dev/docker/trino/conf")
        into("build/trino-conf")
        fileMode = 0b111101101
      }

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
    }

    val init = project.extra.get("initIntegrationTest") as (Test) -> Unit
    init(this)
  }
}

tasks.clean {
  delete("metastore_db")
  delete("target")
  delete("derby.log")
}

tasks.register<JavaExec>("TrinoTest") {
  classpath = sourceSets["test"].runtimeClasspath
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
