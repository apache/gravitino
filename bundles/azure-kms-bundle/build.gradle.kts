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
  implementation(project(":bundles:azure")) {
    isTransitive = false
  }
  implementation(libs.azure.identity)
  implementation(libs.azure.security.keyvault.keys)
  implementation(libs.guava)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  dependencies {
    exclude(dependency("org.slf4j:slf4j-api"))

    // Gravitino interfaces and utilities are supplied by the server.
    exclude(project(":api"))
    exclude(project(":common"))
    exclude(project(":catalogs:catalog-common"))
    exclude(project(":catalogs:hadoop-common"))
  }

  // The source module also contains ADLS filesystem and credential-vending implementations.
  exclude("org/apache/gravitino/abs/**")
  exclude("META-INF/services/org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider")
  exclude("META-INF/services/org.apache.gravitino.credential.CredentialProvider")

  relocate("com.ctc.wstx", "org.apache.gravitino.kms.azure.shaded.com.ctc.wstx")
  relocate("com.fasterxml", "org.apache.gravitino.kms.azure.shaded.com.fasterxml")
  relocate("com.google", "org.apache.gravitino.kms.azure.shaded.com.google")
  relocate("com.microsoft.aad", "org.apache.gravitino.kms.azure.shaded.com.microsoft.aad")
  relocate("com.nimbusds", "org.apache.gravitino.kms.azure.shaded.com.nimbusds")
  relocate("com.sun.jna", "org.apache.gravitino.kms.azure.shaded.com.sun.jna")
  relocate("io.netty", "org.apache.gravitino.kms.azure.shaded.io.netty")
  relocate("net.minidev", "org.apache.gravitino.kms.azure.shaded.net.minidev")
  relocate("net.jcip.annotations", "org.apache.gravitino.kms.azure.shaded.net.jcip.annotations")
  relocate("org.apache.commons", "org.apache.gravitino.kms.azure.shaded.org.apache.commons")
  relocate("org.apache.httpcomponents", "org.apache.gravitino.kms.azure.shaded.org.apache.httpcomponents")
  relocate("org.checkerframework", "org.apache.gravitino.kms.azure.shaded.org.checkerframework")
  relocate("org.codehaus.stax2", "org.apache.gravitino.kms.azure.shaded.org.codehaus.stax2")
  relocate("org.eclipse.jetty", "org.apache.gravitino.kms.azure.shaded.org.eclipse.jetty")
  relocate("org.objectweb.asm", "org.apache.gravitino.kms.azure.shaded.org.objectweb.asm")
  relocate("org.reactivestreams", "org.apache.gravitino.kms.azure.shaded.org.reactivestreams")
  relocate("reactor", "org.apache.gravitino.kms.azure.shaded.reactor")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
