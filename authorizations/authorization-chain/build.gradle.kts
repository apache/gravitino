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
description = "authorization-chain"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark35.get()
val kyuubiVersion: String = libs.versions.kyuubi4authz.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")

dependencies {
  implementation(project(":api")) {
    exclude(group = "*")
  }
  implementation(project(":core")) {
    exclude(group = "*")
  }
  implementation(project(":common")) {
    exclude(group = "*")
  }
  implementation(project(":authorizations:authorization-common")) {
    exclude(group = "*")
  }
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.javax.jaxb.api) {
    exclude("*")
  }
  implementation(libs.javax.ws.rs.api)
  implementation(libs.jettison)
  implementation(libs.rome)
  compileOnly(libs.lombok)

  testImplementation(project(":core"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":catalogs:catalog-common"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":authorizations:authorization-ranger"))
  testImplementation(project(":authorizations:authorization-ranger", "testArtifacts"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testImplementation(libs.testcontainers)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.ranger.intg) {
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
    exclude("org.apache.ranger", "ranger-plugins-audit")
    exclude("org.apache.ranger", "ranger-plugins-cred")
    exclude("org.apache.ranger", "ranger-plugin-classloader")
    exclude("net.java.dev.jna")
    exclude("javax.ws.rs")
    exclude("org.eclipse.jetty")
    exclude("org.apache.hadoop", "hadoop-common")
  }
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }
  testImplementation("org.apache.kyuubi:kyuubi-spark-authz-shaded_$scalaVersion:$kyuubiVersion") {
    exclude("com.sun.jersey")
  }

  testImplementation(libs.hadoop3.client.api)
  testImplementation(libs.hadoop3.client.runtime)

  testImplementation(libs.hadoop3.hdfs) {
    exclude("com.sun.jersey")
    exclude("javax.servlet", "servlet-api")
    exclude("io.netty")
  }
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }

  val copyAuthorizationLibs by registering(Copy::class) {
    dependsOn("jar", runtimeJars)
    from("build/libs") {
      exclude("guava-*.jar")
      exclude("log4j-*.jar")
      exclude("slf4j-*.jar")
    }
    into("$rootDir/distribution/package/authorizations/chain/libs")
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyAuthorizationLibs)
  }

  jar {
    dependsOn(runtimeJars)
  }
}

tasks.test {
  doFirst {
    environment("HADOOP_USER_NAME", "gravitino")
  }
  dependsOn(":catalogs:catalog-hive:jar", ":catalogs:catalog-hive:runtimeJars", ":authorizations:authorization-ranger:jar", ":authorizations:authorization-ranger:runtimeJars")

  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}
