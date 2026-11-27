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
// Match maintenance/jobs and catalog Iceberg 1.11 so IsolatedClassLoader's shared
// org.apache.iceberg.* types include NestedField default APIs used by lakehouse-iceberg.
val sparkVersion: String = libs.versions.spark35.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4spark35.get()

dependencies {
  implementation(project(":api"))
  implementation(project(":maintenance:optimizer-api"))
  implementation(project(":maintenance:updaters"))
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":clients:client-java"))
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(libs.bundles.log4j)
  implementation(libs.commons.cli.new)
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
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":catalogs:catalog-lakehouse-iceberg"))
  testImplementation(project(":server"))
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

  fun isOptimizerLocalJar(name: String): Boolean {
    if (name.startsWith("gravitino-updaters-") || name.startsWith("commons-cli-")) {
      return true
    }
    // gravitino-optimizer-*.jar and gravitino-optimizer-api-*.jar
    return name.startsWith("gravitino-optimizer-")
  }

  fun skipSharedJar(name: String): Boolean {
    return name.endsWith("-tests.jar") ||
      name.endsWith("-sources.jar") ||
      name.endsWith("-javadoc.jar") ||
      name.endsWith("-empty.jar") ||
      name.startsWith("error_prone_annotations-")
  }

  // CLI-owned jars only; Sync trims a previous fat copy.
  register("copyLibs", Sync::class) {
    dependsOn(copyDepends, "build")
    from("build/libs") {
      include(
        "gravitino-optimizer-*.jar",
        "gravitino-updaters-*.jar",
        "commons-cli-*.jar"
      )
      exclude("*-javadoc.jar", "*-sources.jar", "*-empty.jar", "*-tests.jar")
    }
    into("$rootDir/distribution/package/optimizer/libs")
  }

  // Exact jar names to load from ${GRAVITINO_HOME}/libs. Jars not present in root
  // libs (e.g. client-java, qlexpress) stay in optimizer/libs instead.
  val writeSharedLibsList by registering {
    dependsOn(copyDepends, "copyLibs")
    val rootLibsDir = file("$rootDir/distribution/package/libs")
    val optimizerLibsDir = file("$rootDir/distribution/package/optimizer/libs")
    val sharedListFile = file("$rootDir/distribution/package/optimizer/conf/shared-libs")
    outputs.file(sharedListFile)
    val copySubprojectDependencies = rootProject.tasks.findByName("copySubprojectDependencies")
    if (copySubprojectDependencies != null) {
      mustRunAfter(copySubprojectDependencies)
    }
    doLast {
      val runtimeJars =
        configurations.runtimeClasspath.get()
          .files
          .filter { it.name.endsWith(".jar") }
          .filterNot { isOptimizerLocalJar(it.name) }
          .filterNot { skipSharedJar(it.name) }
          .distinctBy { it.name }

      val sharedNames = linkedSetOf<String>()
      for (jar in runtimeJars) {
        if (rootLibsDir.resolve(jar.name).isFile) {
          sharedNames.add(jar.name)
        } else {
          jar.copyTo(optimizerLibsDir.resolve(jar.name), overwrite = true)
        }
      }

      sharedListFile.parentFile.mkdirs()
      sharedListFile.writeText(sharedNames.sorted().joinToString("\n", postfix = "\n"))
    }
  }

  register("copyLibAndConfigs") {
    dependsOn("copyLibs", writeSharedLibsList)
  }
}

configurations.testRuntimeClasspath {
  exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j2-impl")
}

// The lakehouse-iceberg catalog packages runtime jars beside its module jar in build/libs
// (same pattern as jobs → optimizer:copyDepends). Order compileTestJava after that.
tasks.compileTestJava {
  dependsOn(":catalogs:catalog-lakehouse-iceberg:jar", ":catalogs:catalog-lakehouse-iceberg:runtimeJars")
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
    dependsOn(":server:jar")
    // Embedded optimizer integration tests load the Iceberg catalog in an isolated classloader.
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar")
    dependsOn(":catalogs:catalog-lakehouse-iceberg:runtimeJars")
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("copyDepends")
}
