/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
description = "lance-common"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

// Pin the Lance Namespace API to the version Gravitino standardizes on (0.7.5). lance-namespace-hive2
// 0.4.1 is built against lance-namespace 0.7.7 and drags its 0.7.7 core/apache-client in
// transitively; those are excluded from the hive2 dependency below so the whole module resolves to
// the single, consistent 0.7.5 Lance Namespace version Gravitino declares.
configurations.all {
  // Legacy Netty fat-jars (netty-all 4.0.x, netty 3.x) arrive transitively via Hadoop/Hive/
  // lance-core and shadow the modern netty-buffer 4.1.x that Arrow's allocation manager requires,
  // breaking arrow-memory-netty initialization on JDK 17. Arrow uses netty-buffer/netty-common
  // directly, so drop the fat-jars everywhere in this module.
  exclude(group = "io.netty", module = "netty-all")
  exclude(group = "io.netty", module = "netty")
  // Hive (hive-common 2.3.9, pulled transitively by lance-namespace-hive2) drags in an ancient
  // Jetty (org.eclipse.jetty.aggregate:jetty-all 7.6.0, where Container is a *class*) plus
  // mortbay/orbit Jetty. These land in the packaged libs/, and because the distribution globs
  // jars alphabetically, jetty-all-7.6.0 loads before Gravitino's Jetty 9.4.x at runtime ->
  // "class org.eclipse.jetty.server.Connector can not implement ...Container" -
  // IncompatibleClassChangeError that crashes the standalone server at startup. The Hive metastore
  // client needs no servlet container, so drop all legacy Jetty everywhere in this module.
  exclude(group = "org.eclipse.jetty.aggregate")
  exclude(group = "org.eclipse.jetty.orbit")
  exclude(group = "org.mortbay.jetty")
  // Hive/Hadoop also drag in the ancient Servlet 2.4 / JSP 2.0 APIs (javax.servlet:servlet-api:2.4,
  // javax.servlet:jsp-api:2.0). These land in libs/ and, loaded alphabetically before the modern
  // javax.servlet:javax.servlet-api:3.1.0 that Jetty 9.4 requires, shadow it at runtime ->
  // NoSuchMethodError: HttpServletRequest.getDispatcherType() (a Servlet 3.0+ method) on the first
  // request. Drop the obsolete servlet/jsp API jars; the correct javax.servlet-api 3.1.0 remains.
  exclude(group = "javax.servlet", module = "servlet-api")
  exclude(group = "javax.servlet", module = "jsp-api")
  resolutionStrategy {
    // Pin all Arrow modules to Gravitino's version so the arrow-memory-netty allocation manager
    // matches arrow-memory-core regardless of the version lance-namespace-impls-core pulls in.
    force("org.apache.arrow:arrow-memory-netty:${libs.versions.arrow.get()}")
    force("org.apache.arrow:arrow-memory-core:${libs.versions.arrow.get()}")
    force("org.apache.arrow:arrow-vector:${libs.versions.arrow.get()}")
    force("org.apache.arrow:arrow-format:${libs.versions.arrow.get()}")
  }
}

dependencies {
  // Force upgrade for outdated transitive libthrift pulled by Hive Metastore
  constraints {
    implementation(libs.thrift)
  }

  implementation(project(":api"))
  implementation(project(":clients:client-java"))
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":core")) {
    exclude("*")
  }

  // Hive Metastore + Hadoop3 client for the Hive Lance namespace backend.
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

  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jackson.jaxrs.json.provider)
  implementation(libs.arrow.vector)
  implementation(libs.lance.namespace.core) {
    exclude(group = "org.lance", module = "lance-core")
    exclude(group = "com.google.guava", module = "guava") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.core", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.datatype", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.jaxrs", module = "jackson-jaxrs-json-provider") // using gravitino's version
    exclude(group = "org.apache.commons", module = "commons-lang3") // provided by gravitino
    exclude(group = "org.apache.opendal", module = "*")
    exclude(group = "org.junit.jupiter", module = "*")
  }

  // Official Lance Hive 2 namespace implementation. It pulls Hadoop 2.8.5 + an embedded
  // metastore stack (Derby/DataNucleus); exclude those so we run on Gravitino's Hadoop 3
  // metastore client. lance-namespace-impls-core provides LanceTableUtil/RestClient.
  implementation(libs.lance.namespace.hive2) {
    exclude(group = "org.lance", module = "lance-core")
    // lance-namespace-hive2 0.4.1 pulls lance-namespace-core / -apache-client 0.7.7 transitively;
    // exclude them so the 0.7.5 artifacts Gravitino declares (via libs.lance.namespace.core above)
    // are the single Lance Namespace version on the classpath.
    exclude(group = "org.lance", module = "lance-namespace-core")
    exclude(group = "org.lance", module = "lance-namespace-apache-client")
    exclude(group = "org.apache.hadoop")
    exclude(group = "org.apache.derby")
    exclude(group = "org.datanucleus")
    exclude(group = "org.apache.hive", module = "hive-exec")
    exclude(group = "org.apache.hive", module = "hive-service")
    exclude(group = "com.google.guava", module = "guava") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.core", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.datatype", module = "*") // provided by gravitino
    exclude(group = "org.apache.commons", module = "commons-lang3") // provided by gravitino
    // arrow-dataset / arrow-c-data (and their legacy io.netty 3.x/4.0.x transitives) are not used
    // by the Hive namespace; drop them so they don't shadow Gravitino's arrow-vector runtime.
    exclude(group = "org.apache.arrow", module = "arrow-dataset")
    exclude(group = "org.apache.arrow", module = "arrow-c-data")
    exclude(group = "io.netty", module = "netty-all")
    exclude(group = "io.netty", module = "netty")
    exclude(group = "org.slf4j", module = "slf4j-simple")
    exclude(group = "org.junit.jupiter", module = "*")
  }
  implementation(libs.slf4j.api)

  testImplementation(project(":server-common"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testRuntimeOnly(libs.junit.jupiter.engine)
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
