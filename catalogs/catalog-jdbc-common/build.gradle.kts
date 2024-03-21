/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-jdbc-common"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val testJar by tasks.registering(Jar::class) {
  archiveClassifier.set("tests")
  from(sourceSets["test"].output)
}

configurations {
  create("testArtifacts")
}

artifacts {
  add("testArtifacts", testJar)
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":core"))

  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.dbcp2)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)

  testImplementation(libs.commons.io)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)

  testRuntimeOnly(libs.junit.jupiter.engine)
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
}
