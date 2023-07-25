plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
  id("org.nosphere.apache.rat") version "0.8.0"
}

dependencies {
  implementation(project(":api"))
  implementation(project(":meta"))
  implementation(libs.protobuf.java.util) {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Andriod, which we don't want (and breaks multimaps).")
  }
  implementation(libs.substrait.java.core) {
    exclude("org.slf4j")
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
  }
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.commons.io)
  implementation(libs.caffeine)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.mockito.core)
}

tasks.processResources { mustRunAfter("rat") }
tasks.processTestResources { mustRunAfter("rat") }
tasks.compileJava { mustRunAfter("rat") }
tasks.spotlessJava { mustRunAfter("rat") }

tasks.rat {
  substringMatcher("DS", "Datastrato", "Copyright 2023 Datastrato.")
  approvedLicense("Datastrato")
  approvedLicense("Apache License Version 2.0")
  excludes.add("**/*.md")
  excludes.add(".github/**")
  excludes.add("gradle/**")
  excludes.add("**/META-INF/**")
  excludes.add("**/build.gradle.kts")
  excludes.add("**/settings.gradle.kts")
}
