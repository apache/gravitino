/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
}

dependencies {
  compileOnly(libs.hadoop2.common)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.junit4)
  testImplementation(libs.mockito.core)
  testImplementation(libs.hadoop2.minicluster)
}

tasks.build {
  dependsOn("javadoc")
}
