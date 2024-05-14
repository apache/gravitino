/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = project.properties["sparkVersion"] as? String ?: extra["defaultSparkVersion"].toString()
val baseName = "${rootProject.name}-spark-connector-runtime-${sparkVersion}_$scalaVersion"

dependencies {
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  when (sparkVersion) {
    "3.3" -> {
      val kyuubiVersion: String = libs.versions.kyuubi4spark33.get()
      println("Applying Spark 3.3 dependencies")
      implementation(project(":spark-connector:spark3.3"))
      implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
    }
    "3.4" -> {
      val kyuubiVersion: String = libs.versions.kyuubi4spark34.get()
      println("Applying Spark 3.4 dependencies")
      implementation(project(":spark-connector:spark3.4"))
      implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
    }
    "3.5" -> {
      val kyuubiVersion: String = libs.versions.kyuubi4spark35.get()
      println("Applying Spark 3.5 dependencies")
      implementation(project(":spark-connector:spark3.5"))
      implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
    }
    else -> throw IllegalArgumentException("Unsupported Spark version: $sparkVersion")
  }
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveFileName.set("$baseName-$version.jar")
  archiveClassifier.set("")

  // Relocate dependencies to avoid conflicts
  relocate("com.google", "com.datastrato.gravitino.shaded.com.google")
  relocate("google", "com.datastrato.gravitino.shaded.google")
  relocate("org.apache.hc", "com.datastrato.gravitino.shaded.org.apache.hc")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
