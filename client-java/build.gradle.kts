import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import java.util.Locale

/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
  alias(libs.plugins.shadow)
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(libs.protobuf.java.util) {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Andriod, which we don't want (and breaks multimaps).")
  }
  implementation(libs.substrait.java.core) {
    exclude("org.slf4j")
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
    exclude("com.fasterxml.jackson.dataformat")
    exclude("com.google.code.findbugs")
    exclude("com.google.protobuf")
  }
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.httpclient5)
  implementation(libs.commons.lang3)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockserver.netty)
  testImplementation(libs.mockserver.client.java)
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  archiveFileName.set("${rootProject.name.lowercase(Locale.getDefault())}-${project.name}-runtime-${project.version}.jar")

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
