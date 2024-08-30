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

dependencies {
  implementation(project(":api")) {
    exclude(group = "*")
  }
  implementation(project(":core")) {
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
  compileOnly(libs.lombok)
  implementation(libs.mail)
  implementation(libs.ranger.intg) {
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
  }
  implementation(libs.rome)

  testImplementation(project(":common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":catalogs:catalog-common"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testImplementation(libs.testcontainers)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.ranger.intg) {
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.eclipse.jetty")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
    exclude("javax.ws.rs")
    exclude("org.apache.ranger", "ranger-plugin-classloader")
  }
  testImplementation(libs.hive2.jdbc) {
    exclude("org.slf4j")
    exclude("org.eclipse.jetty.aggregate")
  }
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath, "src/main/resources")
    into("build/libs")
    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }
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

  val copyAuthorizationConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/authorizations/ranger/conf")
    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }
    exclude("META-INF")
    fileMode = 0b111101101
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyAuthorizationLibs, copyAuthorizationConfig)
  }

  jar {
    dependsOn(runtimeJars)
  }
}

tasks.test {
  dependsOn(":catalogs:catalog-hive:jar", ":catalogs:catalog-hive:runtimeJars")

  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}
