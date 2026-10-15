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
  annotationProcessor(libs.lombok)

  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":server-common"))

  implementation(libs.bundles.jersey)
  implementation(libs.db.scheduler)
  implementation(libs.guava)
  implementation(libs.mybatis)
  implementation(libs.servlet)

  compileOnly(libs.lombok)
  compileOnly(libs.slf4j.api)

  testImplementation(project(":clients:client-java"))
  testImplementation(project(":core", "testArtifacts"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }

  val copyLibs by registering(Copy::class) {
    dependsOn(jar, runtimeJars)
    from("build/libs") {
      exclude("*-javadoc.jar", "*-sources.jar")
      exclude("guava-*.jar")
      exclude("log4j-*.jar")
      exclude("slf4j-*.jar")
      exclude("error_prone_annotations-*.jar")
    }
    into("$rootDir/distribution/package/libs")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
  }

  register("copyLibAndConfigs", Copy::class) {
    group = "gravitino distribution"
    description = "Copy tms plugin jar and runtime dependencies into distribution package libs"
    dependsOn(copyLibs)
  }

  jar {
    dependsOn(runtimeJars)
  }

  test {
    useJUnitPlatform()
    environment("GRAVITINO_HOME", rootDir.path)
  }
}
