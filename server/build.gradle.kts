/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import java.text.SimpleDateFormat
import java.util.Date

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
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.substrait.java.core) {
    exclude("org.slf4j")
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
  }

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.mockito.core)
}

tasks.register("writeProjectPropertiesFile") {
  val propertiesFile = file("src/main/resources/project.properties")
  val dateFormat = SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

  doLast {
    val compileDate = dateFormat.format(Date())
    val projectVersion = project.version.toString()

    propertiesFile.parentFile.mkdirs()
    propertiesFile.createNewFile()
    propertiesFile.writer().use { writer ->
      writer.write("#\n" +
              "# Copyright 2023 Datastrato.\n" +
              "# This software is licensed under the Apache License version 2.\n" +
              "#\n")
      writer.write("compileDate=$compileDate\n")
      writer.write("version=$projectVersion")
    }
  }
}

tasks.named("build") {
  finalizedBy("writeProjectPropertiesFile")
}
