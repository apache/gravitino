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
val sparkVersion: String = libs.versions.spark33.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4spark.get()
// kyuubi hive connector for Spark 3.3 doesn't support scala 2.13
val kyuubiVersion: String = libs.versions.kyuubi4spark34.get()
val scalaJava8CompatVersion: String = libs.versions.scala.java.compat.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()

dependencies {
  implementation(project(":catalogs:bundled-catalog", configuration = "shadow"))
  implementation(libs.guava)

  compileOnly(project(":clients:client-java-runtime", configuration = "shadow"))
  compileOnly("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  compileOnly("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")

  compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
  compileOnly("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
  compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
  compileOnly("org.scala-lang.modules:scala-java8-compat_$scalaVersion:$scalaJava8CompatVersion")

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  // use log from spark, spark3.3 use low version of log4j, to avoid java.lang.NoSuchMethodError: org.apache.logging.slf4j.Log4jLoggerFactory: method <init>()V not found
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
  testImplementation(project(":server")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":server-common")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":integration-test-common", "testArtifacts"))

  testImplementation(libs.hive2.common) {
    exclude("org.apache.curator")
    // use hadoop from Spark
    exclude("org.apache.hadoop")
    exclude("org.apache.logging.log4j")
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

  testImplementation("org.apache.iceberg:iceberg-core:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-hive-metastore:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  testImplementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  // include spark-sql,spark-catalyst,hive-common,hdfs-client
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion") {
    // conflict with Gravitino server jersey
    exclude("org.glassfish.jersey.core")
    exclude("org.glassfish.jersey.containers")
    exclude("org.glassfish.jersey.inject")
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

val testJar by tasks.registering(Jar::class) {
  archiveClassifier.set("tests")
  from(sourceSets["test"].output)
}

configurations {
  create("testArtifacts")
}

artifacts {
  add("testArtifacts", testJar)
}
