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

configurations.all {
  resolutionStrategy.eachDependency {
    if (requested.group == "org.apache.logging.log4j") {
      throw GradleException("Dependency 'org.apache.logging.log4j' is not allowed.")
    }
  }
}

dependencies {
  implementation(project(":clients:client-java"))

  testImplementation(libs.junit.jupiter.api)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  // Exclude server-side JDBC pooling utilities. These classes reference commons-dbcp2 which is
  // declared compileOnly in :common and therefore never bundled here. Including them produces a
  // broken relocated reference (BasicDataSource renamed to the shaded package, but the actual
  // shaded class absent). Connector runtime JARs placed on Spark/Flink/Hadoop classpaths would
  // then shadow the working copy in gravitino-jobs.jar via parent-first class delegation, causing
  // NoClassDefFoundError at runtime. See https://github.com/apache/gravitino/issues/10508
  exclude("org/apache/gravitino/utils/jdbc/**")

  // Relocate dependencies to avoid conflicts
  relocate("com.google", "org.apache.gravitino.shaded.com.google")
  relocate("com.fasterxml", "org.apache.gravitino.shaded.com.fasterxml")
  relocate("org.apache.httpcomponents", "org.apache.gravitino.shaded.org.apache.httpcomponents")
  relocate("org.apache.commons", "org.apache.gravitino.shaded.org.apache.commons")
  relocate("org.antlr", "org.apache.gravitino.shaded.org.antlr")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.test {
  val shadowJar = tasks.named<ShadowJar>("shadowJar")
  val legalFiles = listOf("LICENSE", "NOTICE").associateWith { name ->
    rootProject.file("$name.bin").takeIf { it.isFile } ?: rootProject.file(name)
  }
  dependsOn(shadowJar)
  inputs.file(shadowJar.flatMap { it.archiveFile })
  inputs.files(legalFiles.values)
  inputs.files(configurations.runtimeClasspath)
  doFirst {
    systemProperty("shadowJarPath", shadowJar.get().archiveFile.get().asFile.absolutePath)
    legalFiles.forEach { (name, file) -> systemProperty("projectLegalFile.$name", file.absolutePath) }
    systemProperty("dependencyJars", configurations.runtimeClasspath.get().asPath)
  }
}
