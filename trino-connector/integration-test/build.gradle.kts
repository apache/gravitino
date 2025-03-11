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
plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

dependencies {
  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))

  testImplementation(libs.awaitility)
  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.jetty)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.commons.cli)
  testImplementation(libs.hadoop2.common) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.hdfs)
  testImplementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  testImplementation(libs.hive2.common) {
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
  }
  testImplementation(libs.httpclient5)
  testImplementation(libs.jline.terminal)
  testImplementation(libs.jodd.core)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.okhttp3.loginterceptor)
  testImplementation(libs.opencsv)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)
  testImplementation(libs.trino.cli)
  testImplementation(libs.trino.client) {
    exclude("jakarta.annotation")
  }

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.register("setupDependencies") {
  dependsOn(":trino-connector:trino-connector:jar")
  dependsOn(":catalogs:catalog-lakehouse-iceberg:jar", ":catalogs:catalog-lakehouse-iceberg:runtimeJars")
  dependsOn(":catalogs:catalog-jdbc-mysql:jar", ":catalogs:catalog-jdbc-mysql:runtimeJars")
  dependsOn(":catalogs:catalog-jdbc-postgresql:jar", ":catalogs:catalog-jdbc-postgresql:runtimeJars")
  dependsOn(":catalogs:catalog-hive:jar", ":catalogs:catalog-hive:runtimeJars")
}

tasks.build {
  dependsOn("setupDependencies")
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    dependsOn("setupDependencies")
    doFirst {
      copy {
        from("${project.rootDir}/dev/docker/trino/conf")
        into("build/trino-conf")
        fileMode = 0b111101101
      }

      // Get current project version
      val version = project.version.toString()
      println("Current project version: $version")

      // Check whether this module has already built
      val trinoConnectorBuildDir = project(":trino-connector:trino-connector").buildDir
      if (trinoConnectorBuildDir.exists()) {
        // Check the version Gravitino related jars in build equal to the current project version
        val invalidGravitinoJars = trinoConnectorBuildDir.resolve("libs").listFiles { _, name -> name.startsWith("gravitino") }?.filter {
          val name = it.name
          !name.endsWith(version + ".jar")
        }

        if (invalidGravitinoJars!!.isNotEmpty()) {
          val message = "Found mismatched versions of gravitino jars in trino-connector/build/libs:\n" +
            "${invalidGravitinoJars.joinToString(", ") { it.name }}\n" +
            "The current version of the project is $version. Please clean the project and rebuild it."
          throw GradleException(message)
        }
      }
    }
  }
}

tasks.register<JavaExec>("TrinoTest") {
  classpath = sourceSets["test"].runtimeClasspath
  mainClass.set("org.apache.gravitino.trino.connector.integration.test.TrinoQueryTestTool")

  if (JavaVersion.current() > JavaVersion.VERSION_1_8) {
    jvmArgs = listOf(
      "--add-opens",
      "java.base/java.lang=ALL-UNNAMED"
    )
  }

  if (project.hasProperty("appArgs")) {
    args = (project.property("appArgs") as String).removeSurrounding("\"").split(" ")
  }
}
