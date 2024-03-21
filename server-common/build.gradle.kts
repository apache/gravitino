/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common")) {
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
  }
  implementation(project(":core"))

  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jwt)
  implementation(libs.bundles.kerby)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.metrics)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.prometheus.servlet)

  testImplementation(libs.commons.io)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.commons.io)
  testImplementation(libs.minikdc) {
    exclude("org.apache.directory.api", "api-ldap-schema-data")
  }
  testRuntimeOnly(libs.junit.jupiter.engine)
}
