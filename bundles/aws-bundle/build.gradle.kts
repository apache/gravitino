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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY
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

// Keeps the Netty artifacts brought in by the AWS SDK on the single version of the catalog. The BOM
// sits in a dedicated configuration so that it only applies to this module.
val nettyBom: Configuration by configurations.creating {
  isCanBeConsumed = false
  isCanBeResolved = false
}

listOf("compileClasspath", "runtimeClasspath", "testCompileClasspath", "testRuntimeClasspath")
  .forEach { configurations.named(it) { extendsFrom(nettyBom) } }

dependencies {
  nettyBom(platform(libs.netty.bom))

  implementation(project(":bundles:aws"))
  implementation(libs.hadoop3.aws) {
    exclude("software.amazon.awssdk", "bundle")
  }
  implementation(libs.bundles.hadoop.aws.sdk)
  implementation(libs.aws.iam)
  implementation(libs.aws.policy)
  implementation(libs.aws.sts)
  // hadoop-aws 3.4 uses Commons Collections 4 (copyFromLocalFile), which hadoop-client-runtime
  // shades for its own use only.
  implementation(libs.commons.collections4)
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.slf4j.api)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  dependencies {
    exclude(dependency("org.slf4j:slf4j-api"))
    // The optional OpenSSL provider contains LGPL code. Hadoop's default mode falls back to JSSE.
    exclude(dependency("org.wildfly.openssl:wildfly-openssl"))

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

  relocate("com.fasterxml.jackson", "org.apache.gravitino.aws.shaded.com.fasterxml.jackson")
  relocate("com.google.common", "org.apache.gravitino.aws.shaded.com.google.common")
  relocate("com.google.errorprone", "org.apache.gravitino.aws.shaded.com.google.errorprone")
  relocate("com.google.thirdparty", "org.apache.gravitino.aws.shaded.com.google.thirdparty")
  relocate("io.netty", "org.apache.gravitino.aws.shaded.io.netty")
  relocate("org.apache.commons", "org.apache.gravitino.aws.shaded.org.apache.commons")
  relocate("org.apache.http", "org.apache.gravitino.aws.shaded.org.apache.http")
  // Apache HTTP client 5, brought by the apache5-client module that the AWS SDK v2 service modules
  // depend on.
  relocate("org.apache.hc", "org.apache.gravitino.aws.shaded.org.apache.hc")
  // hadoop-aws links ConfigureShadedAWSSocketFactory against the Apache HTTP client that the
  // software.amazon.awssdk:bundle shades. The bundle is replaced by the single SDK modules, which use
  // org.apache.http, so map the shaded name to the same relocated package to keep
  // fs.s3a.ssl.channel.mode working.
  relocate(
    "software.amazon.awssdk.thirdparty.org.apache.http",
    "org.apache.gravitino.aws.shaded.org.apache.http"
  )
  relocate("org.checkerframework", "org.apache.gravitino.aws.shaded.org.checkerframework")
  relocate("org.reactivestreams", "org.apache.gravitino.aws.shaded.org.reactivestreams")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-fileset:runtimeJars")
}

tasks.test {
  val bundle = tasks.named<ShadowJar>("shadowJar")
  dependsOn(bundle)
  inputs.file(bundle.flatMap { it.archiveFile })
  systemProperty("nettyVersion", libs.versions.netty.get())
  doFirst { systemProperty("shadowJarPath", bundle.get().archiveFile.get().asFile.absolutePath) }
}
