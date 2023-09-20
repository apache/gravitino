/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

dependencies {
  implementation(project(":clients:client-java"))
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  // Relocate dependencies to avoid conflicts
  relocate("io.substrait", "com.datastrato.graviton.shaded.io.substrait") {
    exclude("org.slf4j")
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
    exclude("com.fasterxml.jackson.dataformat")
    exclude("com.google.code.findbugs")
    exclude("com.google.protobuf")
  }
  relocate("com.google", "com.datastrato.graviton.shaded.com.google")
  relocate("com.fasterxml", "com.datastrato.graviton.shaded.com.fasterxml")
  relocate("org.apache.httpcomponents", "com.datastrato.graviton.shaded.org.apache.httpcomponents")
  relocate("org.apache.commons", "com.datastrato.graviton.shaded.org.apache.commons")
  relocate("org.antlr", "com.datastrato.graviton.shaded.org.antlr")
}

tasks.named("jar") { dependsOn(tasks.named("shadowJar")) }

tasks.jar {
  archiveClassifier.set("empty")
}

