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

plugins {
  `java-library`
  `maven-publish`
}

var trinoVersion = 457
val trinoVersionProvider =
  providers.gradleProperty("trinoVersion").map { it.toInt() }.orElse(trinoVersion)
trinoVersion = trinoVersionProvider.get()

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

  named("build") {
    finalizedBy(copyRuntimeLibs)
  }
}
