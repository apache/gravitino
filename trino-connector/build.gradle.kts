/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(project(":catalogs:bundled-catalog", configuration = "shadow"))
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  // implementation(libs.guava)
  implementation(libs.httpclient5)
  implementation("io.airlift:json:237")
  implementation(libs.trino.jdbc)
  compileOnly(libs.trino.spi) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(libs.awaitility)
  testImplementation(libs.mockito.core)
  testImplementation(libs.trino.memory) {
    exclude("org.antlr")
    exclude("org.apache.logging.log4j")
  }
  testImplementation(libs.trino.testing) {
    exclude("org.apache.logging.log4j")
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

  register("copyLibs", Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs")
    into("$rootDir/distribution/${rootProject.name}-trino-connector")
  }
}
