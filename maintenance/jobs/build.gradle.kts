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

repositories {
  mavenCentral()
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark35.get()

dependencies {
  compileOnly(project(":api"))

  compileOnly(libs.slf4j.api)
  compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.slf4j")
    exclude("org.apache.logging.log4j")
  }

  testImplementation(project(":api"))
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.junit.jupiter.api)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  useJUnitPlatform()
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  archiveClassifier.set("")
  mergeServiceFiles()

  dependencies {
    exclude(dependency("org.apache.spark:.*"))
    exclude(dependency("org.slf4j:slf4j-api"))
    exclude(dependency("org.apache.logging.log4j:.*"))
  }
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
