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
import java.util.zip.ZipFile

description = "paimon-common"

plugins {
  `java-library`
  `maven-publish`
  id("idea")
}

tasks.jacocoTestReport {
  // The resources also contain third-party format classes, which are outside our coverage report.
  classDirectories.setFrom(sourceSets.main.get().output.classesDirs)
}

val bundledPaimonFormat by configurations.creating {
  isCanBeConsumed = false
  isTransitive = false
}

dependencies {
  constraints {
    runtimeOnly(libs.commons.collections3)
  }

  api(libs.bundles.paimonRest) {
    exclude(group = "org.apache.hadoop")
    exclude(group = "org.apache.paimon", module = "paimon-format")
  }
  bundledPaimonFormat(libs.paimonRest.format)
  runtimeOnly(libs.snappy.java)
  runtimeOnly(libs.zstd.jni) {
    because("Paimon uses Zstd for data files and 1.5.7-14 fixes CVE-2026-89046")
  }
  implementation(libs.guava)
  implementation(libs.slf4j.api)
  compileOnly(libs.jsr305)
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)

  runtimeOnly(libs.paimonRest.hive.catalog) {
    // The metastore client shares the Hadoop 3 client API and the host server's Servlet API.
    exclude(group = "org.apache.hadoop")
    exclude(group = "org.apache.paimon", module = "paimon-format")
    exclude(group = "org.eclipse.jetty.aggregate")
    exclude(group = "org.eclipse.jetty.orbit")
    exclude(group = "org.mortbay.jetty")
    exclude(group = "org.apache.tomcat.embed", module = "tomcat-embed-core")
    exclude(group = "javax.servlet")
  }
  runtimeOnly(libs.paimonRest.s3) {
    exclude(group = "org.apache.paimon", module = "paimon-format")
  }
  runtimeOnly("org.apache.paimon:paimon-oss:${libs.versions.paimonRest.get()}") {
    exclude(group = "org.apache.paimon", module = "paimon-format")
  }
  runtimeOnly(libs.postgresql.driver)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.sqlite.jdbc)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

val verifyBundledJtsLicense by tasks.registering {
  val commonJar = tasks.named<Jar>("jar").flatMap { it.archiveFile }
  val jtsLicense = rootProject.layout.projectDirectory.file("licenses/jts-edl.txt")
  dependsOn(tasks.named("jar"))
  inputs.file(commonJar)
  inputs.file(jtsLicense)
  doLast {
    val licensePath = "META-INF/licenses/org.apache.paimon/paimon-format/2.1.0/LICENSE.jts-edl"
    ZipFile(commonJar.get().asFile).use { jar ->
      checkNotNull(jar.getEntry("org/apache/paimon/shade/org/locationtech/jts/geom/Geometry.class")) { "Missing bundled JTS class" }
      val entry = checkNotNull(jar.getEntry(licensePath)) { "Missing bundled JTS license in paimon-common JAR" }
      val text = jar.getInputStream(entry).bufferedReader().use { it.readText() }
      check(text == jtsLicense.asFile.readText()) { "Bundled JTS license differs from the audited distribution text" }
      check(text.contains("Eclipse Distribution License - v 1.0")) { "Missing EDL license text" }
      check(text.contains("Copyright (c) 2003, Open Source Geospatial Foundation.")) { "Missing OSGeo attribution" }
      check(text.contains("Redistributions in binary form must reproduce")) { "Missing license conditions" }
      val inventory = jar.getInputStream(jar.getEntry("META-INF/LICENSE")).bufferedReader().use { it.readText() }
      check(inventory.contains(licensePath)) { "JAR license inventory does not reference the JTS license" }
      check(inventory.contains("JTS 1.20.0 (EDL-1.0 selected)")) { "JAR license inventory does not identify JTS" }
    }
  }
}

tasks.named("check") {
  dependsOn(verifyBundledJtsLicense)
}
