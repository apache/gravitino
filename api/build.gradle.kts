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
  implementation(libs.substrait.java.core) {
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
    exclude("com.fasterxml.jackson.dataformat")
    exclude("com.google.protobuf")
    exclude("com.google.code.findbugs")
    exclude("org.slf4j")
  }
  implementation(libs.guava)
  implementation(libs.slf4j.api)
  implementation(libs.commons.lang3)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
