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
description = "authorization-ranger"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark35.get()
val kyuubiVersion: String = libs.versions.kyuubi4authz.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg.get()
val paimonVersion: String = libs.versions.paimon.get()

dependencies {
  implementation(project(":api")) {
    exclude(group = "*")
  }
  implementation(project(":core")) {
    exclude(group = "*")
  }
  implementation(project(":catalogs:catalog-common")) {
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
  implementation(libs.mail)
  // Gravitino only uses the Ranger Admin REST client (RangerClient) and the model classes. The
  // excluded modules belong to the in-process plugin side (policy engine, audit, credential store,
  // script engines, JWT, user sync) that Gravitino never runs.
  implementation(libs.ranger.intg) {
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
    exclude("org.apache.ranger", "ranger-audit-core")
    exclude("org.apache.ranger", "ranger-plugins-audit")
    exclude("org.apache.ranger", "ranger-plugins-cred")
    exclude("org.apache.ranger", "ranger-plugin-classloader")
    exclude("org.apache.ranger", "ugsync-util")
    exclude("org.graalvm.js")
    exclude("com.nimbusds")
    exclude("net.minidev")
    exclude("net.java.dev.jna")
    exclude("javax.servlet")
    exclude("javax.ws.rs")
    exclude("org.eclipse.jetty")
    // Ranger 2.8 depends on hadoop-client-api/runtime 3.4.x. Keep the Hadoop version shared with
    // the catalogs, which load the authorization libs in the same class loader.
    exclude("org.apache.hadoop")
    // Use the versions Gravitino ships with the server, which the isolated class loader shares.
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.jaxrs")
    exclude("com.fasterxml.jackson.module")
    exclude("org.apache.commons", "commons-lang3")
  }
  // Jackson (including the JAX-RS JSON provider used by RangerRESTClient) is loaded from the
  // Gravitino server classpath at runtime, so it is not shipped in the plugin libs.
  compileOnly(libs.jackson.annotations)
  compileOnly(libs.jackson.databind)
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)

  implementation(libs.rome)
  compileOnly(libs.lombok)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(project(":common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(libs.awaitility)
  testImplementation(libs.gson)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.testcontainers)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.postgresql.driver)
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
  // The Kyuubi Spark authz plugin in the tests runs the Ranger policy engine, which needs
  // ugsync-util since Ranger 2.8. Gravitino itself only uses the Ranger Admin client.
  testRuntimeOnly(libs.ranger.ugsync.util) {
    isTransitive = false
  }

  testImplementation(libs.hadoop3.hdfs) {
    exclude("com.sun.jersey")
    exclude("javax.servlet", "servlet-api")
    exclude("io.netty")
  }
  testImplementation(libs.hadoop3.hdfs.client)
  testImplementation("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  testImplementation("org.apache.paimon:paimon-spark-$sparkMajorVersion:$paimonVersion")
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
    into("$rootDir/distribution/package/authorizations/ranger/libs")
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
  dependsOn(
    ":catalogs:catalog-hive:jar",
    ":catalogs:catalog-hive:runtimeJars",
    ":catalogs:catalog-lakehouse-iceberg:jar",
    ":catalogs:catalog-lakehouse-iceberg:runtimeJars",
    ":catalogs:catalog-lakehouse-paimon:jar",
    ":catalogs:catalog-lakehouse-paimon:runtimeJars",
    ":catalogs:catalog-fileset:jar",
    ":catalogs:catalog-fileset:runtimeJars"
  )

  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}

val testJar by tasks.registering(Jar::class) {
  archiveClassifier.set("tests")
  from(sourceSets["test"].output)
}

configurations {
  create("testArtifacts")
}

artifacts {
  add("testArtifacts", testJar)
}
