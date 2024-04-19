/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  compileOnly(libs.hadoop3.common)
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(libs.caffeine)

  testImplementation(project(":server-common"))
  testImplementation(libs.awaitility)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.hadoop3.common)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockserver.netty) {
    exclude("com.google.guava", "guava")
  }
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.build {
  dependsOn("javadoc")
}

tasks.javadoc {
  dependsOn(":clients:client-java-runtime:javadoc")
  source = sourceSets["main"].allJava +
    project(":clients:client-java-runtime").sourceSets["main"].allJava
}
