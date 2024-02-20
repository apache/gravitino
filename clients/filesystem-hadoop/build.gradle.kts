/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
}

dependencies {
  compileOnly(libs.hadoop2.common)
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))

  testImplementation(libs.hadoop2.minicluster)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit4)
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
