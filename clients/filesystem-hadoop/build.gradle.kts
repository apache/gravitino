/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val defaultHadoopVersion: Int = 3

dependencies {
  if (defaultHadoopVersion == 3) {
    compileOnly(libs.hadoop3.common)
  } else {
    compileOnly(libs.hadoop2.common)
  }
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(libs.caffeine)

  if (defaultHadoopVersion == 3) {
    testImplementation(libs.hadoop3.common)
  } else {
    testImplementation(libs.hadoop2.common)
  }
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
  source = sourceSets["main"].allJava
  classpath = configurations["compileClasspath"]
}
