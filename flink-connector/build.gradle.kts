/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
}

repositories {
  mavenCentral()
}

val flinkVersion: String = libs.versions.flink.get()
val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":clients:client-java"))
  implementation(project(":catalogs:bundled-catalog", configuration = "shadow"))

  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.httpclient5)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)

  implementation("org.apache.flink:flink-connector-hive_$scalaVersion:$flinkVersion")
  implementation("org.apache.flink:flink-table-common:$flinkVersion")
  implementation("org.apache.flink:flink-table-api-java:$flinkVersion")

  implementation(libs.hive2.exec) {
    artifact {
      classifier = "core"
    }
    exclude("com.fasterxml.jackson.core")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.protobuf")
    exclude("org.apache.avro")
    exclude("org.apache.calcite")
    exclude("org.apache.calcite.avatica")
    exclude("org.apache.curator")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.openjdk.jol")
    exclude("org.pentaho")
    exclude("org.slf4j")
  }

  testAnnotationProcessor(libs.lombok)

  testCompileOnly(libs.lombok)
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.testcontainers.mysql)

  testImplementation(libs.hadoop2.common) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  testImplementation(libs.hive2.common) {
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
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
  testImplementation("org.apache.flink:flink-table-api-bridge-base:$flinkVersion") {
    exclude("commons-cli", "commons-cli")
    exclude("commons-io", "commons-io")
    exclude("com.google.code.findbugs", "jsr305")
  }
  testImplementation("org.apache.flink:flink-table-planner_$scalaVersion:$flinkVersion")

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipUTs = project.hasProperty("skipTests")
  if (skipUTs) {
    // Only run integration tests
    include("**/integration/**")
  }

  val skipITs = project.hasProperty("skipITs")
  val skipFlinkITs = project.hasProperty("skipFlinkITs")
  if (skipITs || skipFlinkITs) {
    // Exclude integration tests
    exclude("**/integration/**")
  } else {
    dependsOn(tasks.jar)

    val init = project.extra.get("initIntegrationTest") as (Test) -> Unit
    init(this)
  }
}
