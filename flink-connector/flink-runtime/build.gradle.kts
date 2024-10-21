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
  id("idea")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

val flinkVersion: String = libs.versions.flink.get()
val flinkMajorVersion: String = flinkVersion.substringBeforeLast(".")

// The Flink only support scala 2.12, and all scala api will be removed in a future version.
// You can find more detail at the following issues:
// https://issues.apache.org/jira/browse/FLINK-23986,
// https://issues.apache.org/jira/browse/FLINK-20845,
// https://issues.apache.org/jira/browse/FLINK-13414.
val scalaVersion: String = "2.12"
val artifactName = "gravitino-${project.name}_$scalaVersion"
val baseName = "${rootProject.name}-flink-connector-runtime-${flinkMajorVersion}_$scalaVersion"

dependencies {
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(project(":flink-connector:flink"))
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveFileName.set("$baseName-$version.jar")
  archiveClassifier.set("")

  // Relocate dependencies to avoid conflicts
  relocate("com.google", "org.apache.gravitino.shaded.com.google")
  relocate("google", "org.apache.gravitino.shaded.google")
  relocate("org.apache.hc", "org.apache.gravitino.shaded.org.apache.hc")
}

publishing {
  publications {
    withType<MavenPublication>().configureEach {
      artifactId = baseName
    }
  }
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
