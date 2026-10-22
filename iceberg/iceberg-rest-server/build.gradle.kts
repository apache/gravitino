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

description = "iceberg-rest-service"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark34.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val experimentalScalaVersion = "2.13"
val experimentalSparkVersion: String = libs.versions.spark41.get()
val experimentalSparkMajorVersion: String = experimentalSparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg.get()
val datastratoIcebergVersion: String = libs.versions.datastrato.iceberg.get()
val datastratoIcebergGitCommit: String = libs.versions.experimental.iceberg.git.commit.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()
val caffeineVersion: String = libs.versions.caffeine.get()
val jerseyVersion: String = libs.versions.jersey.get()
val javaxWsRsVersion: String = libs.versions.javax.ws.rs.api.get()
val hk2Version = "2.6.1"
val jakartaValidationVersion = "2.0.2"

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

dependencies {
  implementation(project(":api"))
  implementation(project(":bundles:aliyun"))
  implementation(project(":bundles:aws"))
  implementation(project(":bundles:azure"))
  implementation(project(":bundles:gcp"))
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":clients:client-java"))
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":iceberg:iceberg-common"))
  implementation(project(":server-common")) {
    exclude("*")
  }
  implementation(libs.bundles.iceberg)
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.jwt)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.metrics)
  implementation(libs.bundles.prometheus)
  implementation(libs.caffeine)
  implementation(libs.cglib) {
    // The version of build-in asm is 7.1, which is not compatible with Java 17 well
    exclude("org.ow2.asm")
  }
  implementation(libs.commons.dbcp2)
  implementation(libs.commons.lang3)
  implementation(libs.concurrent.trees)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jcasbin) {
    exclude(group = "com.fasterxml.jackson.core", module = "jackson-databind")
  }
  implementation(libs.metrics.jersey2)
  implementation(libs.mybatis)
  implementation(libs.ognl)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(project(":bundles:iceberg-aliyun-bundle"))
  testImplementation(project(":bundles:iceberg-aws-bundle"))
  testImplementation(project(":bundles:iceberg-gcp-bundle"))
  testImplementation(project(":bundles:iceberg-azure-bundle"))
  testImplementation(project(":core", "testArtifacts"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server-common", "testArtifacts"))
  testImplementation(project(":server"))

  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")
  testImplementation("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion") {
    exclude("org.apache.avro")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
    exclude("io.dropwizard.metrics")
    exclude("org.rocksdb")
  }

  testImplementation(libs.awaitility)
  testImplementation(libs.javax.ws.rs.api)
  // At runtime use the SDK supplied by iceberg-aws-bundle, keeping its S3 and STS versions aligned.
  testCompileOnly(libs.aws.s3)
  testImplementation(libs.h2db)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.slf4j.api)
  testImplementation(libs.testcontainers)
  testImplementation(libs.cglib) {
    // The version of build-in asm is 7.1, which is not compatible with Java 17 well
    exclude("org.ow2.asm")
  }
  testImplementation(libs.asm)
  testImplementation(libs.testcontainers.postgresql)

  // Add Hadoop 3.3+ dependencies since Spark's Hadoop is excluded
  // Required for Iceberg 1.10+ which uses newer Hadoop APIs like FileSystem.openFile()
  testImplementation(libs.hadoop3.client.api)
  testImplementation(libs.hadoop3.client.runtime)

  testRuntimeOnly(libs.junit.jupiter.engine)
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
    into("$rootDir/distribution/package/iceberg-rest-server/libs")
  }

  register("copyLibsToStandalonePackage", Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs")
    into("$rootDir/distribution/gravitino-iceberg-rest-server/libs")
  }

  register("copyConfigs", Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/iceberg-rest-server/conf")

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

  register("copyConfigsToStandalonePackage", Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/gravitino-iceberg-rest-server/conf")

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

  register("copyLibAndConfigs", Copy::class) {
    dependsOn("copyLibs", "copyConfigs")
  }

  register("copyLibAndConfigsToStandalonePackage", Copy::class) {
    dependsOn("copyLibsToStandalonePackage", "copyConfigsToStandalonePackage")
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
  setIncludes(
    setOf(
      "**/IcebergExperimentalAuthorizationIT.class"
    )
  )
  filter {
    includeTestsMatching(
      "org.apache.gravitino.iceberg.integration.test.IcebergExperimentalAuthorizationIT"
    )
  }
  mustRunAfter(tasks.test)
  dependsOn(
    tasks.testClasses,
    tasks.jar,
    ":catalogs:catalog-lakehouse-iceberg:jar",
    ":iceberg:iceberg-rest-server-experiment:copyDepends",
    ":iceberg:iceberg-rest-server-experiment:jar"
  )
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  exclude("**/IcebergExperimentalAuthorizationIT.class")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(
      tasks.jar,
      ":catalogs:catalog-lakehouse-iceberg:jar"
    )
  }
}

tasks.clean {
  delete("spark-warehouse")
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("copyDepends")
}
