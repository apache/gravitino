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
}

repositories {
  mavenCentral()
}

var paimonVersion: String = libs.versions.paimon.get()
val flinkVersion: String = libs.versions.flink.get()
val flinkMajorVersion: String = flinkVersion.substringBeforeLast(".")

val icebergVersion: String = libs.versions.iceberg4connector.get()

// The Flink only support scala 2.12, and all scala api will be removed in a future version.
// You can find more detail at the following issues:
// https://issues.apache.org/jira/browse/FLINK-23986,
// https://issues.apache.org/jira/browse/FLINK-20845,
// https://issues.apache.org/jira/browse/FLINK-13414.
val scalaVersion: String = "2.12"
val artifactName = "${rootProject.name}-flink-${flinkMajorVersion}_$scalaVersion"

dependencies {
  implementation(project(":catalogs:catalog-common")) {
    exclude("org.apache.logging.log4j")
  }
  implementation(libs.guava)

  compileOnly(project(":clients:client-java-runtime", configuration = "shadow"))

  compileOnly("org.apache.iceberg:iceberg-flink-runtime-$flinkMajorVersion:$icebergVersion")
  compileOnly("org.apache.flink:flink-connector-hive_$scalaVersion:$flinkVersion")
  compileOnly("org.apache.flink:flink-table-common:$flinkVersion")
  compileOnly("org.apache.flink:flink-table-api-java:$flinkVersion")
  compileOnly("org.apache.paimon:paimon-flink-1.18:$paimonVersion")
  compileOnly(libs.flinkjdbc)

  compileOnly(libs.hive2.exec) {
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

  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":core"))
  testImplementation(project(":common"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.metrics.core)
  testImplementation(libs.flinkjdbc)

  testImplementation("org.apache.iceberg:iceberg-core:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-hive-metastore:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-flink-runtime-$flinkMajorVersion:$icebergVersion")
  testImplementation("org.apache.flink:flink-connector-hive_$scalaVersion:$flinkVersion")
  testImplementation("org.apache.flink:flink-table-common:$flinkVersion")
  testImplementation("org.apache.flink:flink-table-api-java:$flinkVersion")
  testImplementation("org.apache.paimon:paimon-flink-$flinkMajorVersion:$paimonVersion")

  testImplementation(libs.hive2.exec) {
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

  testImplementation(libs.hadoop2.common) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.hdfs) {
    exclude("com.sun.jersey")
    exclude("commons-cli", "commons-cli")
    exclude("commons-io", "commons-io")
    exclude("commons-codec", "commons-codec")
    exclude("commons-logging", "commons-logging")
    exclude("javax.servlet", "servlet-api")
    exclude("org.mortbay.jetty")
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
    exclude("io.dropwizard.metrics")
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
  testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
    dependsOn(":catalogs:catalog-hive:jar")
  }
}

tasks.withType<Jar> {
  archiveBaseName.set(artifactName)
}

publishing {
  publications {
    withType<MavenPublication>().configureEach {
      artifactId = artifactName
    }
  }
}
