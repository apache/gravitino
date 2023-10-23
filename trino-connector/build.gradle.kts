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
  implementation(project(":clients:client-java-runtime", configuration="shadow"))
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.guava)
  implementation(libs.httpclient5)
  implementation(libs.commons.lang3)
  implementation(libs.trino.spi) {
    exclude("org.apache.logging.log4j")
  }
  implementation(libs.trino.toolkit) {
    exclude("org.apache.logging.log4j")
  }

  testImplementation(libs.trino.testing) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(libs.trino.memory) {
    exclude("org.antlr")
    exclude("org.apache.logging.log4j")
  }
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(17))
  }
}

tasks.named("generateMetadataFileForMavenJavaPublication") {
    dependsOn(":trino-connector:copyDepends")
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
