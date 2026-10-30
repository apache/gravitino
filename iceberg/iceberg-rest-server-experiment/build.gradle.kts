/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

description = "iceberg-rest-server-experiment"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val datastratoIcebergVersion: String = libs.versions.datastrato.iceberg.get()
val caffeineVersion: String = libs.versions.caffeine.get()
val jacksonVersion: String = libs.versions.jackson.get()
val experimentalScalaVersion = "2.13"
val experimentalSparkVersion: String = libs.versions.spark41.get()
val experimentalSparkMajorVersion: String = experimentalSparkVersion.substringBeforeLast(".")
val datastratoIcebergGitCommit: String = libs.versions.experimental.iceberg.git.commit.get()
val jerseyVersion: String = libs.versions.jersey.get()
val javaxWsRsVersion: String = libs.versions.javax.ws.rs.api.get()
val hk2Version = "2.6.1"
val jakartaValidationVersion = "2.0.2"

configurations.configureEach {
  resolutionStrategy {
    force("com.github.ben-manes.caffeine:caffeine:$caffeineVersion")
    force(
      "com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion",
      "com.fasterxml.jackson.core:jackson-core:$jacksonVersion",
      "com.fasterxml.jackson.core:jackson-databind:$jacksonVersion",
      "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion",
      "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion"
    )
    eachDependency {
      if (
        requested.name.startsWith("iceberg-") &&
        (requested.group == "org.apache.iceberg" || requested.group == "com.datastrato")
      ) {
        useTarget("com.datastrato:experimental-${requested.name}:$datastratoIcebergVersion")
        because("Run experimental Iceberg integration tests with the Datastrato distribution")
      }
    }
  }
}

// Keep the Spark runtime isolated from the ordinary unit tests and production classpaths.
val testFixturesSources = sourceSets.create("testFixtures") {
  compileClasspath += sourceSets.main.get().output
  runtimeClasspath += sourceSets.main.get().output
}
val experimentalTestSources = sourceSets.create("experimentalTest") {
  compileClasspath += sourceSets.main.get().output
  runtimeClasspath += sourceSets.main.get().output
}
configurations.named(testFixturesSources.implementationConfigurationName) {
  extendsFrom(configurations.implementation.get())
}
configurations.named(experimentalTestSources.implementationConfigurationName) {
  extendsFrom(configurations.implementation.get())
}
listOf(
  experimentalTestSources.compileClasspathConfigurationName,
  experimentalTestSources.runtimeClasspathConfigurationName
).forEach { configurationName ->
  configurations.named(configurationName) {
    resolutionStrategy {
      // Only Spark ITs use Spark's matching Jackson modules.
      setForcedModules(
        *forcedModules.filterNot { it.group.startsWith("com.fasterxml.jackson.") }.toTypedArray()
      )
      force(
        "javax.ws.rs:javax.ws.rs-api:$javaxWsRsVersion",
        "org.glassfish.jersey.core:jersey-server:$jerseyVersion",
        "org.glassfish.jersey.core:jersey-common:$jerseyVersion",
        "org.glassfish.jersey.core:jersey-client:$jerseyVersion",
        "org.glassfish.jersey.containers:jersey-container-servlet:$jerseyVersion",
        "org.glassfish.jersey.containers:jersey-container-servlet-core:$jerseyVersion",
        "org.glassfish.jersey.containers:jersey-container-jetty-http:$jerseyVersion",
        "org.glassfish.jersey.inject:jersey-hk2:$jerseyVersion",
        "org.glassfish.hk2:hk2-api:$hk2Version",
        "org.glassfish.hk2:hk2-locator:$hk2Version",
        "org.glassfish.hk2:hk2-utils:$hk2Version",
        "org.glassfish.hk2.external:aopalliance-repackaged:$hk2Version",
        "org.glassfish.hk2.external:jakarta.inject:$hk2Version",
        "jakarta.validation:jakarta.validation-api:$jakartaValidationVersion"
      )
    }
  }
}

dependencies {
  compileOnly(libs.jsr305)
  implementation(project(":api"))
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":iceberg:iceberg-common")) {
    exclude("*")
  }
  implementation(project(":iceberg:iceberg-rest-server"))
  implementation(libs.bundles.iceberg)

  testImplementation(testFixturesSources.output)
  testImplementation(project(":server-common"))
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testCompileOnly(libs.jsr305)
  testRuntimeOnly(libs.junit.jupiter.engine)

  add(testFixturesSources.compileOnlyConfigurationName, libs.jsr305)
  add(testFixturesSources.implementationConfigurationName, project(":server-common"))
  val experimentalImplementation = experimentalTestSources.implementationConfigurationName
  // The server uses unshaded Iceberg classes before the Spark runtime's bundled copies.
  add(experimentalImplementation, libs.bundles.iceberg)
  add(experimentalImplementation, testFixturesSources.output)
  add(experimentalImplementation, project(":iceberg:iceberg-rest-server", "testArtifacts"))
  add(experimentalImplementation, project(":integration-test-common", "testArtifacts"))
  add(experimentalImplementation, project(":server"))
  add(experimentalImplementation, project(":server-common"))
  add(experimentalImplementation, project(":clients:client-java"))
  add(experimentalImplementation, "org.apache.spark:spark-sql_$experimentalScalaVersion:$experimentalSparkVersion") {
    exclude(group = "org.apache.avro")
    exclude(group = "org.apache.hadoop")
    exclude(group = "org.apache.zookeeper")
    exclude(group = "io.dropwizard.metrics")
    exclude(group = "org.rocksdb")
  }
  add(experimentalImplementation, "com.datastrato:experimental-iceberg-spark-runtime-${experimentalSparkMajorVersion}_$experimentalScalaVersion:$datastratoIcebergVersion")
  add(experimentalImplementation, "org.scala-lang.modules:scala-collection-compat_$experimentalScalaVersion:${libs.versions.scala.collection.compat.get()}")
  add(experimentalImplementation, libs.hadoop3.client.api)
  add(experimentalImplementation, libs.hadoop3.client.runtime)
  add(experimentalImplementation, libs.javax.ws.rs.api)
  add(experimentalImplementation, libs.awaitility)
  add(experimentalImplementation, libs.commons.lang3)
  add(experimentalImplementation, libs.commons.io)
  add(experimentalImplementation, libs.guava)
  add(experimentalImplementation, libs.slf4j.api)
  add(experimentalImplementation, libs.postgresql.driver)
  add(experimentalImplementation, libs.testcontainers)
  add(experimentalImplementation, libs.testcontainers.postgresql)
  add(experimentalImplementation, libs.junit.jupiter.api)
  add(experimentalImplementation, libs.junit.jupiter.params)
  add(experimentalTestSources.runtimeOnlyConfigurationName, libs.mysql.driver)
  add(experimentalTestSources.runtimeOnlyConfigurationName, libs.junit.jupiter.engine)
}

tasks {
  compileJava {
    dependsOn(":iceberg:iceberg-rest-server:copyDepends")
  }

  val copyDepends by registering(Sync::class) {
    dependsOn(":iceberg:iceberg-rest-server:copyDepends")
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("dependencies"))
  }

  val copyLibs by registering(Sync::class) {
    dependsOn(copyDepends, "build")
    from(jar)
    from(copyDepends)
    into("$rootDir/distribution/package/iceberg-rest-server-experiment/libs")
  }

  val copyConfigs by registering(Copy::class) {
    from(project(":iceberg:iceberg-rest-server").file("src/main/resources"))
    into("$rootDir/distribution/package/iceberg-rest-server-experiment/conf")

    include("core-site.xml.template")
    include("hdfs-site.xml.template")

    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }

    fileMode = 0b111101101
  }

  register("copyLibAndConfigs") {
    dependsOn(copyLibs, copyConfigs)
  }
}

val readRestrictionAuthenticatorJar by tasks.registering(Jar::class) {
  archiveClassifier.set("read-restriction-test-authenticator")
  destinationDirectory.set(layout.buildDirectory.dir("test-fixtures"))
  from(testFixturesSources.output)
}

val experimentalTest by tasks.registering(Test::class) {
  description = "Runs Iceberg REST tests that require the experimental Iceberg runtime"
  group = "verification"
  testClassesDirs = experimentalTestSources.output.classesDirs
  classpath = experimentalTestSources.runtimeClasspath
  systemProperty("datastratoIcebergGitCommit", datastratoIcebergGitCommit)
  systemProperty("spark.ui.enabled", "false")
  // Deploy mode starts a separate server process; expose only the test authenticator fixture.
  environment(
    "GRAVITINO_CLASSPATH",
    readRestrictionAuthenticatorJar.flatMap { it.archiveFile }.get().asFile.absolutePath
  )
  setIncludes(
    setOf(
      "**/IcebergExperimentalAuthorizationIT.class",
      "**/IcebergReadRestrictionIT.class"
    )
  )
  filter {
    includeTestsMatching(
      "org.apache.gravitino.iceberg.integration.test.IcebergExperimentalAuthorizationIT"
    )
    includeTestsMatching(
      "org.apache.gravitino.iceberg.integration.test.IcebergReadRestrictionIT"
    )
  }
  mustRunAfter(tasks.test, ":iceberg:iceberg-rest-server:test")
  dependsOn(
    tasks.named(experimentalTestSources.classesTaskName),
    tasks.jar,
    ":catalogs:catalog-lakehouse-iceberg:jar",
    tasks.named("copyDepends"),
    readRestrictionAuthenticatorJar
  )
}
