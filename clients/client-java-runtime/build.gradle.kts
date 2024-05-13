/*
 * Copyright 2023 Datastrato Pvt Ltd.
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
  relocate("com.google", "com.datastrato.gravitino.shaded.com.google")
  relocate("com.fasterxml", "com.datastrato.gravitino.shaded.com.fasterxml")
  relocate("org.apache.httpcomponents", "com.datastrato.gravitino.shaded.org.apache.httpcomponents")
  relocate("org.apache.commons", "com.datastrato.gravitino.shaded.org.apache.commons")
  relocate("org.apache.logging.slf4j", "com.datastrato.gravitino.shaded.org.apache.logging.slf4j")
  relocate("org.antlr", "com.datastrato.gravitino.shaded.org.antlr")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
