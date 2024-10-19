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
description = "catalog-hive"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark34.get()
val icebergVersion: String = libs.versions.iceberg.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()

dependencies {
  implementation(project(":api")) {
    exclude("*")
  }
  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }
  implementation(project(":catalogs:hive-metastore-common"))
  implementation(project(":core")) {
    exclude("*")
  }

  implementation(libs.commons.collections3)
  implementation(libs.commons.configuration1)
  implementation(libs.htrace.core4)
  implementation(libs.commons.io)
  implementation(libs.guava)
  implementation(libs.hadoop2.auth) {
    exclude("*")
  }
  implementation(libs.woodstox.core)
  implementation(libs.hive2.metastore) {
    exclude("ant")
    exclude("co.cask.tephra")
    exclude("com.github.joshelser")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.code.findbugs", "sr305")
    exclude("com.tdunning", "json")
    exclude("com.zaxxer", "HikariCP")
    exclude("io.dropwizard.metrics")
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
  implementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  implementation(libs.slf4j.api)

  compileOnly(libs.immutables.value)
  compileOnly(libs.lombok)

  annotationProcessor(libs.immutables.value)
  annotationProcessor(libs.lombok)

  testImplementation(project(":catalogs:hive-metastore-common", "testArtifacts"))
  testImplementation(project(":common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))

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

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }

  val copyCatalogLibs by registering(Copy::class) {
    dependsOn("jar", "runtimeJars")
    from("build/libs") {
      exclude("guava-*.jar")
      exclude("log4j-*.jar")
      exclude("slf4j-*.jar")
    }
    into("$rootDir/distribution/package/catalogs/hive/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/hive/conf")

    include("hive.conf")
    include("hive-site.xml.template")

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

    fileMode = 0b111101101
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyCatalogConfig, copyCatalogLibs)
  }
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("runtimeJars")
}
