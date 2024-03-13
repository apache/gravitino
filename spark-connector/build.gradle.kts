/*
 * Copyright 2024 Datastrato Pvt Ltd.
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

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark.get()
val icebergVersion: String = libs.versions.iceberg.get()
val kyuubiVersion: String = libs.versions.kyuubi.get()

dependencies {
  implementation(project(":api"))
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(project(":common"))
  implementation(libs.bundles.log4j)
  implementation(libs.guava)
  implementation("org.apache.iceberg:iceberg-spark-runtime-3.4_$scalaVersion:$icebergVersion")
  implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  implementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
  implementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
