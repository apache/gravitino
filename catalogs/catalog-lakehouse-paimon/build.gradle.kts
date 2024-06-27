/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-lakehouse-paimon"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(libs.bundles.paimon) {
    exclude("com.sun.jersey")
    exclude("javax.servlet")
  }
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.hadoop2.common) {
    exclude("com.github.spotbugs")
    exclude("com.sun.jersey")
    exclude("javax.servlet")
  }
  implementation(libs.hadoop2.hdfs) {
    exclude("com.sun.jersey")
    exclude("javax.servlet")
  }
  implementation(libs.hadoop2.mapreduce.client.core) {
    exclude("com.sun.jersey")
    exclude("javax.servlet")
  }

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(project(":clients:client-java"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.slf4j.api)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.testcontainers)

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
    into("$rootDir/distribution/package/catalogs/lakehouse-paimon/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/lakehouse-paimon/conf")

    include("lakehouse-paimon.conf")
    include("core-site.xml.template")
    include("hdfs-site.xml.template")

    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }

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
      environment("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "datastrato/gravitino-ci-hive:0.1.12")
    }

    val init = project.extra.get("initIntegrationTest") as (Test) -> Unit
    init(this)
  }
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("runtimeJars")
}
