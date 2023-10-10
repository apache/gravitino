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

repositories {
    mavenCentral()
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common")) {
    exclude("org.apache.logging.log4j")
  }
  implementation(project(":clients:client-java")) {
    exclude("org.apache.logging.log4j")
  }
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.guava)
  implementation(libs.httpclient5)
  implementation(libs.commons.lang3)
  implementation(libs.trino.spi)
  implementation(libs.trino.toolkit)
  implementation(libs.substrait.java.core)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.trino.testing)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockserver.netty)
  testImplementation(libs.mockserver.client.java)
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(17))
  }
}

tasks {
    val copyDepends by registering(Copy::class) {
        from(configurations.runtimeClasspath)
        into("build/libs")
    }
    jar {
     finalizedBy(copyDepends)
    }
}
