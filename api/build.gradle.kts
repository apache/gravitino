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
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.build {
  dependsOn("javadoc")
}
