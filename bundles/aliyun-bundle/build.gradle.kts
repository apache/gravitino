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
  implementation(project(":bundles:aliyun"))
  implementation(libs.commons.collections3)
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)
  implementation(libs.hadoop3.oss)
  implementation(libs.httpclient)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")
  mergeServiceFiles()

  dependencies {
    exclude(dependency("org.slf4j:slf4j-api"))
  }

  // Relocate dependencies to avoid conflicts
  relocate("org.jdom", "org.apache.gravitino.aliyun.shaded.org.jdom")
  relocate("org.apache.commons.lang3", "org.apache.gravitino.aliyun.shaded.org.apache.commons.lang3")
  relocate("com.fasterxml.jackson", "org.apache.gravitino.aliyun.shaded.com.fasterxml.jackson")
  relocate("com.google.common", "org.apache.gravitino.aliyun.shaded.com.google.common")
  relocate("org.apache.http", "org.apache.gravitino.aliyun.shaded.org.apache.http")
  relocate("org.apache.commons.collections", "org.apache.gravitino.aliyun.shaded.org.apache.commons.collections")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
}
