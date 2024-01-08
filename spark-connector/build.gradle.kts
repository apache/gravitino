/*
 * Copyright 2023 Datastrato Pvt Ltd.
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

dependencies {
  implementation(project(":clients:client-java"))
  implementation(project(":api"))
  /*implementation(project(":core"))*/
  implementation(project(":common"))
  implementation(libs.guava)
  implementation(libs.iceberg.spark.runtime)
  implementation(libs.bundles.spark)
  implementation(libs.bundles.log4j)
  implementation(libs.kyuubi.spark.connector)
  implementation(libs.scala.library)
  testImplementation(libs.mockito.core)
}
