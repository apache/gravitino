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
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":client-java"))
  implementation(project(":server"))

  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
  testImplementation(libs.guava)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.httpclient5)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val integrationTest by creating(Test::class) {
    environment("GRAVITON_HOME", rootDir.path + "/distribution/package")
    useJUnitPlatform()
  }
}