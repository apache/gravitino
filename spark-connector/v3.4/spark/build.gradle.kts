/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark34.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4spark.get()
val kyuubiVersion: String = libs.versions.kyuubi4spark34.get()
val scalaJava8CompatVersion: String = libs.versions.scala.java.compat.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()

dependencies {
  implementation(project(":spark-connector:spark-common"))
  compileOnly("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion") {
    exclude("com.fasterxml.jackson")
  }
  compileOnly(project(":clients:client-java-runtime", configuration = "shadow"))
  compileOnly("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")

  testImplementation(project(":api")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":clients:client-java")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":core")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":common")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":server-common")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":spark-connector:spark-common", "testArtifacts")) {
    exclude("com.fasterxml.jackson")
  }

  testImplementation(libs.hive2.common) {
    exclude("com.sun.jersey")
    exclude("org.apache.curator")
    exclude("org.apache.logging.log4j")
    // use hadoop from Spark
    exclude("org.apache.hadoop")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
  }

  testImplementation(libs.hive2.metastore) {
    exclude("co.cask.tephra")
    exclude("com.github.joshelser")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.code.findbugs", "sr305")
    exclude("com.tdunning", "json")
    exclude("com.zaxxer", "HikariCP")
    exclude("com.sun.jersey")
    exclude("io.dropwizard.metricss")
    exclude("javax.transaction", "transaction-api")
    exclude("org.apache.avro")
    exclude("org.apache.curator")
    exclude("org.apache.hbase")
    exclude("org.apache.hadoop")
    exclude("org.apache.hive", "hive-common")
    exclude("org.apache.hive", "hive-shims")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.slf4j")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.testcontainers)

  // org.apache.iceberg.rest.RESTSerializers#registerAll(ObjectMapper) has different method signature for iceberg-core and iceberg-spark-runtime package, we must make sure iceberg-core is in front to start up MiniGravitino server.
  testImplementation("org.apache.iceberg:iceberg-core:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-hive-metastore:$icebergVersion")
  testImplementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  // include spark-sql,spark-catalyst,hive-common,hdfs-client
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion") {
    // conflict with Gravitino server jersey
    exclude("org.glassfish.jersey.core")
    exclude("org.glassfish.jersey.containers")
    exclude("org.glassfish.jersey.inject")
    exclude("com.sun.jersey")
    exclude("com.fasterxml.jackson")
    exclude("com.fasterxml.jackson.core")
  }
  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipUTs = project.hasProperty("skipTests")
  if (skipUTs) {
    // Only run integration tests
    include("**/integration/**")
  }

  val skipITs = project.hasProperty("skipITs")
  val skipSparkITs = project.hasProperty("skipSparkITs")
  if (skipITs || skipSparkITs) {
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

tasks.clean {
  delete("spark-warehouse")
}
