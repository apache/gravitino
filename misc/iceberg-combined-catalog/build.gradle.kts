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

dependencies {
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.iceberg)
  implementation(libs.commons.lang3)
  implementation(libs.commons.io)
  implementation(libs.iceberg.hive.metastore)
  implementation(libs.sqlite.jdbc)

  implementation(libs.hive2.metastore) {
    exclude("org.apache.avro", "avro")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.pentaho") // missing dependency
    exclude("org.apache.hbase")
    exclude("org.apache.logging.log4j")
    exclude("co.cask.tephra")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("com.tdunning", "json")
    exclude("javax.transaction", "transaction-api")
    exclude("com.zaxxer", "HikariCP")
    exclude("org.apache.zookeeper")
    exclude("org.apache.hadoop", "hadoop-yarn-server-common")
    exclude("org.apache.hadoop", "hadoop-yarn-server-applicationhistoryservice")
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")
    exclude("org.apache.hadoop", "hadoop-yarn-api")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("com.github.spotbugs")
  }

  implementation(libs.hadoop2.hdfs)
  implementation(libs.hadoop2.common) {
    exclude("com.github.spotbugs")
  }
  implementation(libs.hadoop2.mapreduce.client.core)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.mockito.core)
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }
}

tasks {
}
