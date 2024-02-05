/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":meta"))
  implementation(project(":common"))
  implementation(libs.protobuf.java.util) {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Andriod, which we don't want (and breaks multimaps).")
  }
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.commons.io)
  implementation(libs.caffeine)
  implementation(libs.rocksdbjni)
  implementation(libs.bundles.metrics)
  implementation(libs.bundles.prometheus)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)

  testRuntimeOnly(libs.junit.jupiter.engine)
}
