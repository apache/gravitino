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
  implementation(project(":common"))
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.httpclient5)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.protobuf.java.util) {
      exclude("com.google.guava", "guava")
          .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
  }

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(libs.bundles.jwt)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockserver.netty)
  testImplementation(libs.mockserver.client.java)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.build {
  dependsOn("javadoc")
}

tasks.javadoc {
  dependsOn(":api:javadoc", ":common:javadoc")
  source =
    sourceSets["main"].allJava +
    project(":api").sourceSets["main"].allJava +
    project(":common").sourceSets["main"].allJava

  classpath = configurations["compileClasspath"] +
    project(":api").configurations["runtimeClasspath"] +
    project(":common").configurations["runtimeClasspath"]
}
