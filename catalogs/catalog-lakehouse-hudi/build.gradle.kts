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
description = "catalog-lakehouse-iceberg"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val fullSparkVersion: String = libs.versions.spark34.get()
val sparkVersion = fullSparkVersion.split(".").take(2).joinToString(".")

dependencies {
  implementation(project(":api")) {
    exclude(group = "*")
  }
  implementation(project(":common")) {
    exclude(group = "*")
  }
  implementation(project(":catalogs:hive-metastore-common"))
  implementation(project(":core")) {
    exclude(group = "*")
  }

  implementation(libs.guava)
  implementation(libs.hive2.exec) {
    artifact {
      classifier = "core"
    }
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.protobuf")
    exclude("org.apache.avro")
    exclude("org.apache.ant")
    exclude("org.apache.calcite")
    exclude("org.apache.calcite.avatica")
    exclude("org.apache.curator")
    exclude("org.apache.derby")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.hive", "hive-llap-tez")
    exclude("org.apache.hive", "hive-vector-code-gen")
    exclude("org.apache.ivy")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.zookeeper")
    exclude("org.codehaus.groovy", "groovy-all")
    exclude("org.datanucleus", "datanucleus-core")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.openjdk.jol")
    exclude("org.pentaho")
    exclude("org.slf4j")
  }
  implementation(libs.hive2.metastore) {
    exclude("ant")
    exclude("co.cask.tephra")
    exclude("com.github.joshelser")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.code.findbugs", "sr305")
    exclude("com.tdunning", "json")
    exclude("com.zaxxer", "HikariCP")
    exclude("io.dropwizard.metricss")
    exclude("javax.transaction", "transaction-api")
    exclude("org.apache.ant")
    exclude("org.apache.avro")
    exclude("org.apache.curator")
    exclude("org.apache.derby")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.hbase")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.zookeeper")
    exclude("org.datanucleus")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.openjdk.jol")
    exclude("org.slf4j")
  }
  implementation(libs.hadoop2.common) {
    exclude("*")
  }
  implementation(libs.slf4j.api)
  implementation(libs.thrift)

  compileOnly(libs.lombok)

  annotationProcessor(libs.lombok)

  testImplementation(project(":catalogs:hive-metastore-common", "testArtifacts"))

  testImplementation(libs.bundles.log4j)
  testImplementation(libs.commons.collections3)
  testImplementation(libs.commons.configuration1)
  testImplementation(libs.datanucleus.core)
  testImplementation(libs.datanucleus.api.jdo)
  testImplementation(libs.datanucleus.rdbms)
  testImplementation(libs.datanucleus.jdo)
  testImplementation(libs.derby)
  testImplementation(libs.hadoop2.auth) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  testImplementation(libs.htrace.core4)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.woodstox.core)
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$fullSparkVersion") {
    exclude("org.apache.hadoop")
    exclude("io.dropwizard.metrics")
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.module", "jackson-module-scala_2.12")
  }
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$fullSparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }

  testRuntimeOnly("org.apache.hudi:hudi-spark$sparkVersion-bundle_$scalaVersion:0.15.0")
  testRuntimeOnly(libs.junit.jupiter.engine)
}
