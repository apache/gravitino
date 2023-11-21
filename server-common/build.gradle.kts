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

dependencies {
  implementation(project(":common")) {
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
  }
  implementation(project(":api"))
  implementation(project(":core"))

  implementation(libs.guava)
  implementation(libs.commons.lang3)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jwt)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.mockito.core)
  testImplementation(libs.commons.io)
}
