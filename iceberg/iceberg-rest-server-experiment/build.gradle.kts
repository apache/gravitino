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
import org.gradle.api.attributes.Attribute

description = "iceberg-rest-server-experiment"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val datastratoIcebergVersion: String = libs.versions.datastrato.iceberg.get()
val caffeineVersion: String = libs.versions.caffeine.get()
val jacksonVersion: String = libs.versions.jackson.get()
val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
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
    // Spark supplies matching Jackson modules on the test classpaths.
    if (!name.contains("test", ignoreCase = true)) {
      force(
        "com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion",
        "com.fasterxml.jackson.core:jackson-core:$jacksonVersion",
        "com.fasterxml.jackson.core:jackson-databind:$jacksonVersion",
        "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion",
        "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion"
      )
    }
    eachDependency {
      if (requested.group == "org.apache.spark") {
        val experimentalSparkModule = requested.name.replace("_$scalaVersion", "_$experimentalScalaVersion")
        useTarget("${requested.group}:$experimentalSparkModule:$experimentalSparkVersion")
        because("The experimental Iceberg Spark runtime requires Spark 4.1 with Scala 2.13")
      } else if (
        requested.group == "org.scala-lang.modules" && requested.name.endsWith("_$scalaVersion")
      ) {
        val experimentalScalaModule = requested.name.replace("_$scalaVersion", "_$experimentalScalaVersion")
        useTarget("${requested.group}:$experimentalScalaModule:${requested.version}")
        because("Spark 4.1 uses Scala 2.13")
      } else if (requested.name.startsWith("iceberg-spark-runtime-")) {
        useTarget(
          "com.datastrato:experimental-iceberg-spark-runtime-${experimentalSparkMajorVersion}_$experimentalScalaVersion:$datastratoIcebergVersion"
        )
        because("Run experimental Iceberg integration tests with the matching Spark runtime")
      } else if (
        requested.name.startsWith("iceberg-") &&
        (requested.group == "org.apache.iceberg" || requested.group == "com.datastrato")
      ) {
        useTarget("com.datastrato:experimental-${requested.name}:$datastratoIcebergVersion")
        because("Run experimental Iceberg integration tests with the Datastrato distribution")
      }
    }
  }
}

val testRuntimeClasspath = configurations.testRuntimeClasspath.get()
val experimentalTestRuntimeClasspath by configurations.creating {
  isCanBeConsumed = false
  isCanBeResolved = true
  extendsFrom(testRuntimeClasspath)
  attributes {
    testRuntimeClasspath.attributes.keySet().forEach { key ->
      @Suppress("UNCHECKED_CAST")
      val typedKey = key as Attribute<Any>
      attribute(typedKey, testRuntimeClasspath.attributes.getAttribute(typedKey)!!)
    }
  }
  resolutionStrategy {
    force(
      "com.github.ben-manes.caffeine:caffeine:$caffeineVersion",
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

  testImplementation(project(":iceberg:iceberg-rest-server", "testArtifacts"))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testCompileOnly(libs.jsr305)
  testRuntimeOnly(libs.junit.jupiter.engine)
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
  from(sourceSets.test.get().output) {
    include("org/apache/gravitino/server/authentication/ReadRestrictionGroupAuthenticator.class")
  }
}

val experimentalTest by tasks.registering(Test::class) {
  description = "Runs Iceberg REST tests that require the experimental Iceberg runtime"
  group = "verification"
  testClassesDirs = sourceSets.test.get().output.classesDirs
  classpath =
    sourceSets.main.get().output +
    sourceSets.test.get().output +
    experimentalTestRuntimeClasspath
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
    tasks.testClasses,
    tasks.jar,
    ":catalogs:catalog-lakehouse-iceberg:jar",
    tasks.named("copyDepends"),
    readRestrictionAuthenticatorJar
  )
}

tasks.test {
  exclude("**/IcebergExperimentalAuthorizationIT.class")
  exclude("**/IcebergReadRestrictionIT.class")
}
