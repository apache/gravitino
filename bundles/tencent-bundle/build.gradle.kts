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
  implementation(project(":bundles:tencent"))

  implementation(libs.commons.collections3)
  implementation(libs.commons.io)
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)
  implementation(libs.hadoop3.cos)
  implementation(libs.httpclient)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")
  mergeServiceFiles()

  dependencies {
    exclude(dependency("org.slf4j:slf4j-api"))

    // Exclude Gravitino modules to prevent class duplication and "Split Packages" issues.
    // These modules (api, common, catalogs) are already provided by the Gravitino server and gravitino-filesystem-hadoop3-runtime.
    // Including them here would cause the Relocation rules below to incorrectly modify
    // method signatures (e.g., JsonUtils.anyFieldMapper returning a shaded ObjectMapper),
    // leading to java.lang.NoSuchMethodError at runtime.
    exclude(project(":api"))
    exclude(project(":common"))
    exclude(project(":catalogs:catalog-common"))
    exclude(project(":catalogs:hadoop-common"))
  }

  // Relocate dependencies to avoid conflicts.
  // hadoop-cos (from com.qcloud.cos:hadoop-cos) bundles the qcloud-cos SDK and a few common
  // libraries; relocate them under "org.apache.gravitino.tencent.shaded.*" following the
  // same pattern as the aws/aliyun/azure/gcp bundles.
  relocate("com.fasterxml.jackson", "org.apache.gravitino.tencent.shaded.com.fasterxml.jackson")
  relocate("com.google", "org.apache.gravitino.tencent.shaded.com.google")
  relocate("com.qcloud", "org.apache.gravitino.tencent.shaded.com.qcloud")
  relocate("org.apache.commons", "org.apache.gravitino.tencent.shaded.org.apache.commons")
  relocate("org.apache.http", "org.apache.gravitino.tencent.shaded.org.apache.http")
  relocate("org.checkerframework", "org.apache.gravitino.tencent.shaded.org.checkerframework")
  relocate("org.jacoco.agent.rt", "org.apache.gravitino.tencent.shaded.org.jacoco.agent.rt")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-fileset:runtimeJars")
}
