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
plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark34.get()
val icebergVersion: String = libs.versions.iceberg.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()

dependencies {
  testImplementation(project(":catalogs:hive-metastore-common", "testArtifacts"))
  testImplementation(project(":common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(project(":catalogs:hadoop-common")) {
    exclude("*")
  }

  testImplementation(libs.bundles.jetty)
  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.hadoop2.hdfs)
  testImplementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  testImplementation(libs.hive2.common) {
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)

  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion") {
    exclude("org.apache.hadoop")
  }
  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }
  testImplementation(libs.slf4j.api)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.localstack)
  testImplementation(libs.hadoop2.aws)
  testImplementation(libs.hadoop3.abs)
  testImplementation(libs.hadoop3.gcs)

  // You need this to run test CatalogHiveABSIT as it required hadoop3 environment introduced by hadoop3.abs
  // (The protocol `abfss` was first introduced in Hadoop 3.2.0), However, as the there already exists
  // hadoop2.common in the test classpath, If we added the following dependencies directly, it will
  // cause the conflict between hadoop2 and hadoop3, resulting test failures, so we comment the
  // following line temporarily, if you want to run the test, please uncomment it.
  // In the future, we may need to refactor the test to avoid the conflict.
  // testImplementation(libs.hadoop3.common)

  testRuntimeOnly(libs.junit.jupiter.engine)
}
