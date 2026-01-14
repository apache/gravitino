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

import net.ltgt.gradle.errorprone.errorprone

plugins {
  id("java")
  id("idea")
}

repositories {
  mavenCentral()
}

var trinoVersion = 435
val trinoVersionProvider =
  providers.gradleProperty("trinoVersion").map { it.toInt() }.orElse(435)
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

tasks.named("generateMetadataFileForMavenJavaPublication") {
  // No extra dependencies required now that runtime artifacts are copied directly during distribution tasks.
}

tasks {
  register("copyLibs", Copy::class) {
    dependsOn("build")
    from("build/libs")
    from({ configurations.runtimeClasspath.get().filter(File::isFile) })
    into("$rootDir/distribution/${rootProject.name}-trino-connector")
  }
}

tasks.withType<JavaCompile>().configureEach {
  // Error Prone is incompatible with the JDK 24 toolchain required by this Trino range.
  options.errorprone.isEnabled.set(false)
  options.release.set(17)
}
