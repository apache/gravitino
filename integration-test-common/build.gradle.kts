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
  id("java")
}

repositories {
  mavenCentral()
}

dependencies {
  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":lance:lance-common")) {
    exclude("*")
  }
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.bundles.jetty)
  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.commons.cli)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.commons.io)
  testImplementation(libs.guava)
  testImplementation(libs.httpclient5)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.ranger.intg) {
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
    exclude("org.apache.hadoop", "hadoop-common")
  }
  testImplementation(libs.hadoop3.client.api)
  testImplementation(libs.hadoop3.client.runtime)

  testImplementation(platform("org.junit:junit-bom:5.9.1"))
  testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
  useJUnitPlatform()
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
