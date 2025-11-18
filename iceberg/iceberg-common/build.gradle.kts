/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
description = "iceberg-common"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(libs.bundles.iceberg)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.kerby) {
    exclude("org.jline")
  }
  implementation(libs.caffeine)
  implementation(libs.cglib)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.iceberg.aliyun)
  implementation(libs.iceberg.aws)
  implementation(libs.iceberg.azure)
  implementation(libs.iceberg.hive.metastore)
  implementation(libs.iceberg.gcp)
  // Upgrade to Hadoop 3.3+ for Iceberg 1.10 compatibility
  // Iceberg 1.10 requires Hadoop 3.3+ APIs like FileSystem.openFile() and FsTracer.get()
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)
  // use hdfs-default.xml
  implementation(libs.hadoop3.hdfs) {
    exclude("*")
  }
  implementation(libs.hive2.metastore) {
    exclude("co.cask.tephra")
    exclude("com.github.spotbugs")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.sun.jersey")
    exclude("com.tdunning", "json")
    exclude("com.zaxxer", "HikariCP")
    exclude("com.github.joshelser")
    exclude("io.dropwizard.metrics")
    exclude("javax.servlet")
    exclude("javax.transaction", "transaction-api")
    exclude("jline")
    exclude("org.apache.ant")
    exclude("org.apache.avro", "avro")
    exclude("org.apache.curator")
    exclude("org.apache.derby")
    exclude("org.apache.hbase")
    exclude("org.apache.hive", "hive-service-rpc")
    exclude("org.apache.hadoop")
    exclude("org.apache.hadoop", "hadoop-yarn-api")
    exclude("org.apache.hadoop", "hadoop-yarn-server-applicationhistoryservice")
    exclude("org.apache.hadoop", "hadoop-yarn-server-common")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.orc")
    exclude("org.apache.zookeeper")
    exclude("org.datanucleus")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.mortbay.jetty")
    exclude("org.pentaho") // missing dependency
    exclude("org.slf4j", "slf4j-log4j12")
  }

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(project(":server-common"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.sqlite.jdbc)

  testRuntimeOnly(libs.junit.jupiter.engine)
}
