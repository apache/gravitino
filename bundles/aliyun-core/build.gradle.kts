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
  compileOnly(project(":core"))
  compileOnly(project(":catalogs:catalog-common"))
  compileOnly(project(":catalogs:catalog-hadoop"))
  compileOnly(libs.hadoop3.client.api)
  compileOnly(libs.hadoop3.client.runtime)
  compileOnly(libs.hadoop3.oss)

  implementation(libs.aliyun.credentials.sdk)

  // Aliyun oss SDK depends on this package, and JDK >= 9 requires manual add
  // https://www.alibabacloud.com/help/en/oss/developer-reference/java-installation?spm=a2c63.p38356.0.i1
  implementation(libs.sun.activation)

  // oss needs StringUtils from commons-lang3 or the following error will occur in 3.3.0
  // java.lang.NoClassDefFoundError: org/apache/commons/lang3/StringUtils
  // org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore.initialize(AliyunOSSFileSystemStore.java:111)
  // org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem.initialize(AliyunOSSFileSystem.java:323)
  // org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3611)
  implementation(libs.commons.lang3)
  implementation(libs.httpclient)
  implementation(libs.commons.collections3)

  implementation(libs.guava)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)

  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }
  implementation(project(":catalogs:hadoop-common")) {
    exclude("*")
  }
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")
  mergeServiceFiles()

  // Relocate dependencies to avoid conflicts
  relocate("org.jdom", "org.apache.gravitino.shaded.org.jdom")
  relocate("org.apache.commons.lang3", "org.apache.gravitino.shaded.org.apache.commons.lang3")
  relocate("com.fasterxml.jackson", "org.apache.gravitino.shaded.com.fasterxml.jackson")
  relocate("com.google.common", "org.apache.gravitino.shaded.com.google.common")
  relocate("org.apache.http", "org.apache.gravitino.shaded.org.apache.http")
  relocate("org.apache.commons.collections", "org.apache.gravitino.shaded.org.apache.commons.collections")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
}
