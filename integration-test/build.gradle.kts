/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
}

dependencies {
  testImplementation(project(":api"))
  testImplementation(project(":core"))
  testImplementation(project(":common"))
  testImplementation(project(":client-java"))
  testImplementation(project(":catalog-hive"))
  testImplementation(project(":server"))

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
    exclude("com.google.code.findbugs", "jsr305")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.curator")
    exclude("org.pentaho")
    exclude("org.slf4j")
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
  testImplementation(libs.hive2.common)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.httpclient5)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.mockito.core)
}

tasks {
  val integrationTest by creating(Test::class) {
    environment("GRAVITON_HOME", rootDir.path + "/distribution/package")
    environment("HADOOP_USER_NAME", "hive")
    environment("HADOOP_HOME", "/tmp")
    useJUnitPlatform()
  }
}