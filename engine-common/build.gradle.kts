/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

plugins {
  id("java")
}

group = "org.example"
version = "0.4.0-SNAPSHOT"

repositories {
  mavenCentral()
}

dependencies {
  testImplementation(platform("org.junit:junit-bom:5.9.1"))
  testImplementation("org.junit.jupiter:junit-jupiter")

  implementation(project(":catalogs:catalog-hive")) {
    exclude("*")
  }

  implementation(project(":core")) {
    exclude("*")
  }
  implementation(libs.slf4j.api)
  implementation(libs.commons.collections4)
}

tasks.test {
  useJUnitPlatform()
}
