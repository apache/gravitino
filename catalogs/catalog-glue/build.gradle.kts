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
description = "catalog-glue"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":common"))
  compileOnly(project(":core"))

  compileOnly(libs.lombok)

  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }

  implementation(libs.aws.glue)
  implementation(libs.aws.sts)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  annotationProcessor(libs.lombok)

  testImplementation(project(":api"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testImplementation(libs.slf4j.api)

  testRuntimeOnly(libs.junit.jupiter.engine)

  testImplementation(libs.testcontainers)
  testImplementation(project(":integration-test-common", "testArtifacts"))
}

tasks {
  register("runtimeJars", Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }

  val copyCatalogLibs by registering(Copy::class) {
    dependsOn("jar", "runtimeJars")
    from("build/libs") {
      exclude("guava-*.jar")
      exclude("log4j-*.jar")
      exclude("slf4j-*.jar")
      exclude("error_prone_annotations-*.jar")
    }
    into("$rootDir/distribution/package/catalogs/glue/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/glue/conf")

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
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("runtimeJars")
}
