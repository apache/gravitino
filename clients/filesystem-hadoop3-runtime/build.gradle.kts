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

// Everything packaged into the shaded runtime jar. Declaring these as `implementation` would make
// Gradle publish them as `runtime` scoped POM dependencies and list them in the Gradle module
// metadata, so resolving the coordinate would download the modules and third-party libraries the
// jar already contains. See https://github.com/apache/gravitino/issues/13171
val shadedDependencies by configurations.creating {
  isCanBeConsumed = false
  isCanBeResolved = true
}

dependencies {
  shadedDependencies(project(":bundles:aliyun"))
  shadedDependencies(project(":bundles:aws"))
  shadedDependencies(project(":bundles:azure"))
  shadedDependencies(project(":bundles:gcp"))
  shadedDependencies(project(":bundles:tencent"))
  shadedDependencies(project(":clients:filesystem-hadoop3")) {
    exclude(group = "org.slf4j")
  }
  shadedDependencies(project(":clients:client-java-runtime", configuration = "shadow"))
  shadedDependencies(libs.commons.lang3)
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(shadedDependencies)
  archiveClassifier.set("")

  // Strip shaded slf4j-api brought in by :clients:client-java-runtime — Hadoop classpaths
  // usually ship slf4j 1.7.x, and bundling 2.x here breaks that binding at runtime.
  exclude("org/slf4j/**")
  exclude("META-INF/maven/org.slf4j/**")
  exclude("META-INF/services/org.slf4j.spi.SLF4JServiceProvider")

  // Relocate dependencies to avoid conflicts
  relocate("com.google", "org.apache.gravitino.shaded.com.google") {
    // Do not relocate com.google.cloud.hadoop classes — they come from the external
    // gcs-connector jar on the user's Hadoop classpath and must keep their original package name.
    exclude("com.google.cloud.hadoop.**")
  }
  relocate("com.github.benmanes.caffeine", "org.apache.gravitino.shaded.com.github.benmanes.caffeine")
  // relocate common lang3 package
  relocate("org.apache.commons.lang3", "org.apache.gravitino.shaded.org.apache.commons.lang3")
  relocate("org.apache.hc", "org.apache.gravitino.shaded.org.apache.hc")
  relocate("org.checkerframework", "org.apache.gravitino.shaded.org.checkerframework")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
