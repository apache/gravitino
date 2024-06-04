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
val sparkVersion: String = libs.versions.spark34.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val baseName = "spark-connector-runtime-${sparkMajorVersion}_$scalaVersion"

dependencies {
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  when (sparkMajorVersion) {
    "3.4" -> {
      val kyuubiVersion: String = libs.versions.kyuubi4spark34.get()
      implementation(project(":spark-connector:spark-3.4"))
      implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
    }
    else -> throw IllegalArgumentException("Unsupported Spark version: $sparkMajorVersion")
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

publishing {
  publications {
    withType<MavenPublication>().configureEach {
      artifactId = baseName
    }
  }
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
