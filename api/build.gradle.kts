plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
  id("org.nosphere.apache.rat") version "0.8.0"
}

dependencies {
  implementation(libs.substrait.java.core) {
    exclude("org.slf4j")
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
    exclude("com.fasterxml.jackson.dataformat")
    exclude("com.google.protobuf")
  }
  implementation(libs.guava)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.processResources { mustRunAfter("rat") }
tasks.processTestResources { mustRunAfter("rat") }
tasks.compileJava { mustRunAfter("rat") }
tasks.spotlessJava { mustRunAfter("rat") }

tasks.rat {
  substringMatcher("DS", "Datastrato", "Copyright 2023 Datastrato.")
  approvedLicense("Datastrato")
  approvedLicense("Apache License Version 2.0")
}