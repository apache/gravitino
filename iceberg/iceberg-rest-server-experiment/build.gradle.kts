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
description = "iceberg-rest-server-experiment"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

val datastratoIcebergVersion: String = libs.versions.datastrato.iceberg.get()
val caffeineVersion: String = libs.versions.caffeine.get()
val jacksonVersion: String = libs.versions.jackson.get()

configurations.configureEach {
  resolutionStrategy {
    force(
      "com.github.ben-manes.caffeine:caffeine:$caffeineVersion",
      "com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion",
      "com.fasterxml.jackson.core:jackson-core:$jacksonVersion",
      "com.fasterxml.jackson.core:jackson-databind:$jacksonVersion",
      "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion",
      "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion"
    )
    eachDependency {
      if (
        requested.name.startsWith("iceberg-") &&
        (requested.group == "org.apache.iceberg" || requested.group == "com.datastrato")
      ) {
        useTarget("com.datastrato:experimental-${requested.name}:$datastratoIcebergVersion")
        because("Use the Datastrato Iceberg distribution in the experimental auxiliary service")
      }
    }
  }
}

dependencies {
  implementation(project(":iceberg:iceberg-rest-server"))
  implementation(libs.bundles.iceberg)
}

tasks {
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
