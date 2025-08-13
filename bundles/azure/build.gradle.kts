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
  compileOnly(libs.hadoop3.abs)
  compileOnly(libs.hadoop3.client.api)
  compileOnly(libs.hadoop3.client.runtime)

  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }
  implementation(project(":catalogs:hadoop-common")) {
    exclude("*")
  }

  implementation(libs.azure.identity)
  implementation(libs.azure.storage.file.datalake)

  implementation(libs.commons.lang3)
  // runtime used
  implementation(libs.commons.logging)
  implementation(libs.guava)

  testImplementation(project(":api"))
  testImplementation(project(":core"))
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
  relocate("com.azure", "org.apache.gravitino.azure.shaded.com.azure")
  relocate("com.ctc.wstx", "org.apache.gravitino.azure.shaded.com.ctc.wstx")
  relocate("com.fasterxml", "org.apache.gravitino.azure.shaded.com.fasterxml")
  relocate("com.google", "org.apache.gravitino.azure.shaded.com.google.common")
  relocate("com.microsoft.aad", "org.apache.gravitino.azure.shaded.com.microsoft.aad")
  relocate("com.nimbusds", "org.apache.gravitino.azure.shaded.com.nimbusds")
  relocate("com.sun.jna", "org.apache.gravitino.azure.shaded.com.sun.jna")
  relocate("com.sun.xml", "org.apache.gravitino.azure.shaded.com.sun.xml")
  relocate("io.netty", "org.apache.gravitino.azure.shaded.io.netty")
  relocate("net.minidev", "org.apache.gravitino.azure.shaded.net.minidev")
  relocate("net.jcip.annotations", "org.apache.gravitino.azure.shaded.net.jcip.annotations")
  relocate("org.apache.commons", "org.apache.gravitino.azure.shaded.org.apache.commons")
  relocate("org.apache.httpcomponents", "org.apache.gravitino.azure.shaded.org.apache.httpcomponents")
  relocate("org.checkerframework", "org.apache.gravitino.azure.shaded.org.checkerframework")
  relocate("org.codehaus.stax2", "org.apache.gravitino.azure.shaded.org.codehaus.stax2")
  relocate("org.eclipse.jetty", "org.apache.gravitino.azure.shaded.org.eclipse.jetty")
  relocate("org.objectweb.asm", "org.apache.gravitino.azure.shaded.org.objectweb.asm")
  relocate("org.reactivestreams", "org.apache.gravitino.azure.shaded.org.reactivestreams")
  relocate("reactor", "org.apache.gravitino.azure.shaded.reactor")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-fileset:runtimeJars")
}
