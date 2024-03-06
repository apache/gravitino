import org.gradle.internal.os.OperatingSystem

/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-hadoop"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":core"))
  implementation(project(":common"))

  implementation(libs.guava)
  implementation(libs.hadoop2.common) {
    exclude("com.sun.jersey")
    exclude("javax.servlet", "servlet-api")
  }

  implementation(libs.hadoop2.hdfs) {
    exclude("javax.servlet", "servlet-api")
    exclude("com.sun.jersey")
  }

  implementation(libs.slf4j.api)

  testImplementation(project(":catalogs:catalog-common", "testArtifacts"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))

  testImplementation(libs.bundles.log4j)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
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
    into("$rootDir/distribution/package/catalogs/hadoop/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/hadoop/conf")

    include("hadoop.conf")
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
    dependsOn(copyCatalogConfig, copyCatalogLibs)
  }
}

tasks.test {
  exclude("**/lakehouse/**")
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/docker/**")
  } else {
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar", ":catalogs:catalog-lakehouse-iceberg:runtimeJars")
    dependsOn(tasks.jar)

    doFirst {
      jvmArgs(project.property("extraJvmArgs") as List<*>)

      // Default use MiniGravitino to run integration tests
      environment("GRAVITINO_ROOT_DIR", rootDir.path)
      environment("IT_PROJECT_DIR", buildDir.path)
      environment("HADOOP_USER_NAME", "datastrato")
      environment("HADOOP_HOME", "/tmp")
      environment("PROJECT_VERSION", version)

      val dockerRunning = project.rootProject.extra["dockerRunning"] as? Boolean ?: false
      val macDockerConnector = project.rootProject.extra["macDockerConnector"] as? Boolean ?: false
      if (OperatingSystem.current().isMacOsX() &&
        dockerRunning &&
        macDockerConnector
      ) {
        environment("NEED_CREATE_DOCKER_NETWORK", "true")
      }

      // Gravitino CI Docker image
      environment("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "datastrato/gravitino-ci-hive:0.1.8")
      environment("GRAVITINO_CI_TRINO_DOCKER_IMAGE", "datastrato/gravitino-ci-trino:0.1.5")

      // Change poll image pause time from 30s to 60s
      environment("TESTCONTAINERS_PULL_PAUSE_TIMEOUT", "60")

      val testMode = project.properties["testMode"] as? String ?: "embedded"
      systemProperty("gravitino.log.path", buildDir.path + "/${project.name}-integration-test.log")
      delete(buildDir.path + "/${project.name}-integration-test.log")
      if (testMode == "deploy") {
        environment("GRAVITINO_HOME", rootDir.path + "/distribution/package")
        systemProperty("testMode", "deploy")
      } else if (testMode == "embedded") {
        environment("GRAVITINO_HOME", rootDir.path)
        environment("GRAVITINO_TEST", "true")
        environment("GRAVITINO_WAR", rootDir.path + "/web/dist/")
        systemProperty("testMode", "embedded")
      } else {
        throw GradleException("Gravitino integration tests only support [-PtestMode=embedded] or [-PtestMode=deploy] mode!")
      }

      useJUnitPlatform {
        val DOCKER_IT_TEST = project.rootProject.extra["docker_it_test"] as? Boolean ?: false
        if (!DOCKER_IT_TEST) {
          excludeTags("gravitino-docker-it")
        }
      }
    }
  }
}
