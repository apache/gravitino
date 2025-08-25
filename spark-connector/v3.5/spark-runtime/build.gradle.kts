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
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark35.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val baseName = "${rootProject.name}-spark-connector-runtime-${sparkMajorVersion}_$scalaVersion"

dependencies {
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  when (sparkMajorVersion) {
    "3.5" -> {
      val kyuubiVersion: String = libs.versions.kyuubi4spark.get()
      implementation(project(":spark-connector:spark-3.5"))
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

  exclude("org/slf4j/**")

  // Relocate dependencies to avoid conflicts
  relocate("com.google", "org.apache.gravitino.shaded.com.google")
  relocate("google", "org.apache.gravitino.shaded.google")
  relocate("org.apache.hc", "org.apache.gravitino.shaded.org.apache.hc")
  relocate("com.github.benmanes.caffeine", "org.apache.gravitino.shaded.com.github.benmanes.caffeine")
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
