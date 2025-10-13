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
description = "catalog-fileset"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api")) {
    exclude(group = "*")
  }
  implementation(project(":catalogs:catalog-common")) {
    exclude(group = "*")
  }
  implementation(project(":catalogs:hadoop-common")) {
    exclude(group = "*")
  }
  implementation(project(":common")) {
    exclude(group = "*")
  }
  implementation(project(":core")) {
    exclude(group = "*")
  }
  implementation(libs.awaitility)
  implementation(libs.caffeine)
  implementation(libs.commons.lang3)
  implementation(libs.commons.io)
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)
  implementation(libs.slf4j.api)
  implementation(libs.metrics.caffeine)
  implementation(libs.metrics.core)

  compileOnly(libs.guava)

  testImplementation(project(":clients:client-java"))
  testImplementation(project(":bundles:aws-bundle", configuration = "shadow"))
  testImplementation(project(":bundles:gcp-bundle", configuration = "shadow"))
  testImplementation(project(":bundles:aliyun-bundle", configuration = "shadow"))
  testImplementation(project(":bundles:azure-bundle", configuration = "shadow"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.hadoop3.gcs)
  testImplementation(libs.hadoop3.minicluster)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.minikdc)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)

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
      exclude("slf4j-*.jar")
      exclude("guava-*.jar")
      exclude("curator-*.jar")
      exclude("netty-*.jar")
      exclude("snappy-*.jar")
      exclude("zookeeper-*.jar")
      exclude("jetty-*.jar")
      exclude("javax.servlet-*.jar")
      exclude("kerb-*.jar")
      exclude("kerby-*.jar")
    }
    into("$rootDir/distribution/package/catalogs/fileset/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/fileset/conf")

    include("fileset.conf")
    include("core-site.xml.template")
    include("hdfs-site.xml.template")

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

  // this task depends on :bundles:aws-bundle:jar
  dependsOn(":bundles:aws-bundle:jar")
  dependsOn(":bundles:aliyun-bundle:jar")
  dependsOn(":bundles:gcp-bundle:jar")
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("runtimeJars")
}
