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
  compileOnly(project(":catalogs:catalog-hadoop"))
  compileOnly(libs.hadoop3.common)
  implementation(libs.hadoop3.oss)

  // oss needs StringUtils from commons-lang or the following error will occur in 3.1.0
  // java.lang.NoClassDefFoundError: org/apache/commons/lang/StringUtils
  // org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore.initialize(AliyunOSSFileSystemStore.java:111)
  // org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem.initialize(AliyunOSSFileSystem.java:323)
  // org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3611)
  implementation(libs.commons.lang)
  implementation(project(":catalogs:catalog-common")) {
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
  relocate("org.apache.commons.lang", "org.apache.gravitino.shaded.org.apache.commons.lang")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
}
