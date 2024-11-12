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

dependencies {
  compileOnly(project(":clients:client-java-runtime", configuration = "shadow"))
  compileOnly(libs.hadoop3.common)
  implementation(project(":catalogs:catalog-hadoop")) {
    exclude(group = "*")
  }
  implementation(project(":catalogs:catalog-common")) {
    exclude(group = "*")
  }

  implementation(libs.caffeine)

  testImplementation(project(":api"))
  testImplementation(project(":core"))
  testImplementation(project(":common"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":bundles:gcp-bundle"))
  testImplementation(project(":bundles:aliyun-bundle"))
  testImplementation(project(":bundles:aws-bundle"))
  testImplementation(libs.awaitility)
  testImplementation(libs.bundles.jetty)
  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.testcontainers)
  testImplementation(libs.guava)
  testImplementation(libs.hadoop3.client)
  testImplementation(libs.hadoop3.common) {
    exclude("com.sun.jersey")
    exclude("javax.servlet", "servlet-api")
  }
  testImplementation(libs.hadoop3.hdfs) {
    exclude("com.sun.jersey")
    exclude("javax.servlet", "servlet-api")
    exclude("io.netty")
  }
  testImplementation(libs.httpclient5)
  testImplementation(libs.javax.jaxb.api) {
    exclude("*")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.minikdc)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockserver.netty) {
    exclude("com.google.guava", "guava")
  }
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.build {
  dependsOn("javadoc")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-hadoop:jar")
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    dependsOn(":catalogs:catalog-hadoop:jar", ":catalogs:catalog-hadoop:runtimeJars")
  }

  // this task depends on :bundles:aws-bundle:shadowJar
  dependsOn(":bundles:aws-bundle:jar")
  dependsOn(":bundles:aliyun-bundle:jar")
  dependsOn(":bundles:gcp-bundle:jar")
}

tasks.javadoc {
  dependsOn(":clients:client-java-runtime:javadoc")
  source = sourceSets["main"].allJava +
    project(":clients:client-java-runtime").sourceSets["main"].allJava
}

tasks.clean {
  delete("target")
}
