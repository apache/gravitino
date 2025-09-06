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
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

dependencies {
  compileOnly(project(":api"))
  compileOnly(libs.hadoop3.client.api)
  compileOnly(libs.hadoop3.client.runtime)
  compileOnly(libs.hadoop3.gcs)
  compileOnly(project(":common"))

  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }
  implementation(project(":catalogs:hadoop-common")) {
    exclude("*")
  }
  implementation(libs.commons.lang3)
  // runtime used
  implementation(libs.commons.logging)
  implementation(libs.google.auth.credentials)
  implementation(libs.google.auth.http)

  testImplementation(project(":api"))
  testImplementation(project(":core"))
  testImplementation(project(":common"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  dependencies {
    exclude(dependency("org.slf4j:slf4j-api"))
  }

  // Relocate dependencies to avoid conflicts
  relocate("com.google.api", "org.apache.gravitino.gcp.shaded.com.google.api")
  relocate("com.google.auth", "org.apache.gravitino.gcp.shaded.com.google.auth")
  relocate("com.google.auto", "org.apache.gravitino.gcp.shaded.com.google.auto")
  relocate("com.google.common", "org.apache.gravitino.gcp.shaded.com.google.common")
  relocate("com.google.errorprone", "org.apache.gravitino.gcp.shaded.com.google.errorprone")
  relocate("com.google.gson", "org.apache.gravitino.gcp.shaded.com.google.gson")
  relocate("com.google.j2objc", "org.apache.gravitino.gcp.shaded.com.google.j2objc")
  relocate("com.google.thirdparty", "org.apache.gravitino.gcp.shaded.com.google.thirdparty")
  relocate("io.grpc", "org.apache.gravitino.gcp.shaded.io.grpc")
  relocate("io.opencensus", "org.apache.gravitino.gcp.shaded.io.opencensus")
  relocate("org.apache.commons", "org.apache.gravitino.gcp.shaded.org.apache.commons")
  relocate("org.apache.http", "org.apache.gravitino.gcp.shaded.org.apache.http")
  relocate("org.apache.httpcomponents", "org.apache.gravitino.gcp.shaded.org.apache.httpcomponents")
  relocate("org.checkerframework", "org.apache.gravitino.gcp.shaded.org.checkerframework")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-fileset:runtimeJars")
}
