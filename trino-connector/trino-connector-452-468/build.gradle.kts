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

import com.diffplug.gradle.spotless.SpotlessExtension
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.internal.hash.ChecksumService
import org.gradle.kotlin.dsl.support.serviceOf

plugins {
  `java-library`
  `maven-publish`
}

// This module supports Trino versions 452-468
val minTrinoVersion = 452
val maxTrinoVersion = 468
val otelSemconvVersion = "1.32.0"

val trinoVersion = providers.gradleProperty("trinoVersion")
  .map { it.trim().toInt() }
  .orElse(minTrinoVersion)
  .get()

// Validate version range
check(trinoVersion in minTrinoVersion..maxTrinoVersion) {
  "Module ${project.path} supports Trino versions $minTrinoVersion-$maxTrinoVersion, " +
    "but trinoVersion=$trinoVersion was specified. " +
    "Please set '-PtrinoVersion=$minTrinoVersion' (or any version in the supported range)."
}

java {
  toolchain.languageVersion.set(JavaLanguageVersion.of(24))
}

dependencies {
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(libs.airlift.json)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  implementation("io.trino:trino-jdbc:$trinoVersion")
  runtimeOnly("io.opentelemetry.semconv:opentelemetry-semconv:$otelSemconvVersion")
  compileOnly(libs.airlift.resolver)
  compileOnly("io.trino:trino-spi:$trinoVersion") {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(libs.awaitility)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mysql.driver)
  testImplementation("io.trino:trino-memory:$trinoVersion") {
    exclude("org.antlr")
    exclude("org.apache.logging.log4j")
  }
  testImplementation("io.trino:trino-testing:$trinoVersion") {
    exclude("org.apache.logging.log4j")
  }
  testRuntimeOnly(libs.junit.jupiter.engine)
}

sourceSets {
  main {
    java.srcDirs("../trino-connector/src/main/java")
  }
  test {
    java.srcDirs("../trino-connector/src/test/java")
    resources.srcDirs("../trino-connector/src/test/resources")
  }
}

plugins.withId("com.diffplug.spotless") {
  configure<SpotlessExtension> {
    java {
      // Keep Spotless within this module to avoid cross-project target errors.
      target(project.fileTree("src") { include("**/*.java") })
    }
  }
}

tasks.withType<JavaCompile>().configureEach {
  // Error Prone is incompatible with the JDK 24 toolchain required by this Trino range.
  options.errorprone.isEnabled.set(false)
  options.release.set(17)
}

tasks.withType<Test>().configureEach {
  extensions
    .findByType(org.gradle.testing.jacoco.plugins.JacocoTaskExtension::class.java)
    ?.isEnabled = false
}

tasks {
  val copyRuntimeLibs by registering(Copy::class) {
    dependsOn("jar")
    from({ configurations.runtimeClasspath.get().filter(File::isFile) })
    into(layout.buildDirectory.dir("libs"))
  }

  val distributionDir = rootProject.layout.projectDirectory.dir("distribution/${rootProject.name}-${project.name}")

  val copyLibs by registering(Copy::class) {
    dependsOn(copyRuntimeLibs, "build")
    from(layout.buildDirectory.dir("libs"))
    from(rootProject.layout.projectDirectory.dir("licenses")) {
      into("licenses")
    }
    from(rootProject.file("LICENSE.trino"))
    from(rootProject.file("NOTICE.trino"))
    from(rootProject.file("README.md"))
    into(distributionDir)
    rename { fileName ->
      fileName.replace(".trino", "")
    }
    outputs.dir(distributionDir)
  }

  val assembleTrinoConnector by registering(Tar::class) {
    dependsOn(copyLibs)
    group = "gravitino distribution"
    finalizedBy("checksumTrinoConnector")
    val archiveBase = "${rootProject.name}-${project.name}-$version"
    into(archiveBase)
    from(distributionDir)
    compression = Compression.GZIP
    archiveFileName.set("$archiveBase.tar.gz")
    destinationDirectory.set(rootProject.layout.projectDirectory.dir("distribution"))
  }

  val checksumTrinoConnector by registering {
    group = "gravitino distribution"
    dependsOn(assembleTrinoConnector)
    val archiveFile = assembleTrinoConnector.flatMap { it.archiveFile }
    val checksumFile = archiveFile.map { archive ->
      archive.asFile.let { it.resolveSibling("${it.name}.sha256") }
    }
    inputs.file(archiveFile)
    outputs.file(checksumFile)
    doLast {
      checksumFile.get().writeText(
        serviceOf<ChecksumService>().sha256(archiveFile.get().asFile).toString()
      )
    }
  }

  named("build") {
    finalizedBy(copyRuntimeLibs)
  }
}
