/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
}

repositories {
    mavenCentral()
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":client-java"))
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.httpclient5)
  implementation(libs.commons.lang3)
  implementation("io.trino:trino-plugin-toolkit:425")
  implementation("io.trino:trino-spi:425")
  implementation(libs.substrait.java.core)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockserver.netty)
  testImplementation(libs.mockserver.client.java)
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(17))
  }
}