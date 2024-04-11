/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-jdbc-doris"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":catalogs:catalog-jdbc-common"))
  implementation(project(":common"))
  implementation(project(":core"))

  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jsqlparser)
  implementation(libs.slf4j.api)

  testImplementation(project(":catalogs:catalog-jdbc-common", "testArtifacts"))
  testImplementation(project(":integration-test-common", "testArtifacts"))

  testImplementation(libs.commons.lang3)
  testImplementation(libs.guava)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }
  val copyCatalogLibs by registering(Copy::class) {
    dependsOn("jar", "runtimeJars")
    from("build/libs")
    into("$rootDir/distribution/package/catalogs/jdbc-doris/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/jdbc-doris/conf")

    include("jdbc-doris.conf")

    exclude { details ->
      details.file.isDirectory()
    }
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyCatalogLibs, copyCatalogConfig)
  }
}

tasks.test {
  val skipUTs = project.hasProperty("skipTests")
  if (skipUTs) {
    // Only run integration tests
    include("**/integration/**")
  }

  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/**")
  } else {
    dependsOn(tasks.jar)

    doFirst {
      environment("GRAVITINO_CI_DORIS_DOCKER_IMAGE", "datastrato/gravitino-ci-doris:0.1.1")
    }

    val init = project.extra.get("initIntegrationTest") as (Test) -> Unit
    init(this)
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("runtimeJars")
}
