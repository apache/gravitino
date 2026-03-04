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
description = "Gravitino Optimizer"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String =
  project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark33.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4connector.get()

dependencies {
  implementation(project(":api"))
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":clients:client-java"))
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.guava)
  implementation(libs.commons.dbcp2)
  implementation(libs.ql.expression)
  implementation(libs.h2db)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.awaitility)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation("org.slf4j:slf4j-api:1.7.36")
  testRuntimeOnly("org.slf4j:slf4j-simple:1.7.36")
  testRuntimeOnly(
    "org.scala-lang.modules:scala-collection-compat_$scalaVersion:${libs.versions.scala.collection.compat.get()}"
  )
  testImplementation(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion"
  ) {
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
  }
  testImplementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion") {
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
  }
  testImplementation("org.apache.spark:spark-core_$scalaVersion:$sparkVersion") {
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
  }
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude(group = "org.slf4j", module = "slf4j-api")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
  }
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)
  testRuntimeOnly(libs.mysql.driver)
  testRuntimeOnly(libs.postgresql.driver)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }
  jar {
    finalizedBy(copyDepends)
  }

  register("copyLibs", Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs")
    into("$rootDir/distribution/package/optimizer/libs")
  }

  register("copyConfigs", Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/optimizer/conf")

    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }

    fileMode = 0b111101101
  }

  register("copyLibAndConfigs", Copy::class) {
    dependsOn("copyLibs", "copyConfigs")
  }
}

configurations.testRuntimeClasspath {
  exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j2-impl")
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
    dependsOn(":server:jar")
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("copyDepends")
}
