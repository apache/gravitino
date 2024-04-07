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
val sparkVersion: String = libs.versions.spark.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg.get()
val kyuubiVersion: String = libs.versions.kyuubi.get()
val scalaJava8CompatVersion: String = libs.versions.scala.java.compat.get()

dependencies {
  implementation(project(":catalogs:bundled-catalog", configuration = "shadow"))
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(libs.guava)
  implementation("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")

  compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
  compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
  compileOnly("org.scala-lang.modules:scala-java8-compat_$scalaVersion:$scalaJava8CompatVersion")
  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
  testImplementation("org.scala-lang.modules:scala-java8-compat_$scalaVersion:$scalaJava8CompatVersion")
  testRuntimeOnly(libs.junit.jupiter.engine)
}
