/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-jdbc-postgresql"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":api"))
  implementation(project(":catalogs:catalog-jdbc-common"))
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.commons.collections4)
  implementation(libs.jsqlparser)

  testImplementation(project(":catalogs:catalog-jdbc-common"))
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.guava)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs_all")
  }
  val copyCatalogLibs by registering(Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs_all", "build/libs")
    into("$rootDir/distribution/package/catalogs/jdbc-postgresql/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/jdbc-postgresql/conf")

    include("jdbc-postgresql.conf")

    exclude { details ->
      details.file.isDirectory()
    }
  }

  val copyLibAndConfig by registering(Copy::class) {
    dependsOn(copyCatalogLibs, copyCatalogConfig)
  }
}
