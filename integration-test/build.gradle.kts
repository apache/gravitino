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
  `application`
  id("java")
  id("idea")
}

dependencies {
  testAnnotationProcessor(libs.lombok)

  testCompileOnly(libs.lombok)

  testImplementation(project(":api"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":clients:filesystem-hadoop3"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))

  testImplementation(libs.awaitility)
  testImplementation(libs.commons.cli)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.guava)
  testImplementation(libs.commons.io)
  testImplementation(libs.bundles.jetty)
  testImplementation(libs.bundles.jersey)
  testImplementation(libs.bundles.jwt)
  testImplementation(libs.bundles.log4j)
  testImplementation(libs.hadoop2.common) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  testImplementation(libs.hadoop2.hdfs)
  testImplementation(libs.hive2.common) {
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
  }
  testImplementation(libs.hive2.exec) {
    artifact {
      classifier = "core"
    }
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.avro")
    exclude("org.apache.zookeeper")
    exclude("com.google.protobuf")
    exclude("org.apache.calcite")
    exclude("org.apache.calcite.avatica")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.curator")
    exclude("org.pentaho")
    exclude("org.slf4j")
  }
  testImplementation(libs.hive2.jdbc) {
    exclude("org.slf4j")
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
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.slf4j")
  }
  testImplementation(libs.httpclient5)
  testImplementation(libs.jline.terminal)
  testImplementation(libs.jodd.core)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.minikdc) {
    exclude("org.apache.directory.api", "api-ldap-schema-data")
  }
  testImplementation(libs.mockito.core)
  testImplementation(libs.mybatis)
  testImplementation(libs.mysql.driver)

  testImplementation(libs.okhttp3.loginterceptor)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.rauschig)
  testImplementation(libs.selenium)
  testImplementation(libs.slf4j.jdk14)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.trino.cli)
  testImplementation(libs.trino.client) {
    exclude("jakarta.annotation")
  }
  testImplementation(libs.trino.jdbc)
  testImplementation(libs.ranger.intg) {
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
  }
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    val skipWebITs = project.hasProperty("skipWebITs")
    if (skipWebITs) {
      exclude("**/integration/test/web/ui/**")
    }

    dependsOn(":trino-connector:jar")
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar", ":catalogs:catalog-lakehouse-iceberg:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-doris:jar", ":catalogs:catalog-jdbc-doris:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-mysql:jar", ":catalogs:catalog-jdbc-mysql:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-postgresql:jar", ":catalogs:catalog-jdbc-postgresql:runtimeJars")
    dependsOn(":catalogs:catalog-hadoop:jar", ":catalogs:catalog-hadoop:runtimeJars")
    dependsOn(":catalogs:catalog-hive:jar", ":catalogs:catalog-hive:runtimeJars")
    dependsOn(":catalogs:catalog-kafka:jar", ":catalogs:catalog-kafka:runtimeJars")

    // Frontend tests depend on the web page, so we need to build the web module first.
    dependsOn(":web:build")

    doFirst {
      // Gravitino CI Docker image
      environment("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "datastrato/gravitino-ci-hive:0.1.13")
      environment("GRAVITINO_CI_TRINO_DOCKER_IMAGE", "datastrato/gravitino-ci-trino:0.1.5")
      environment("GRAVITINO_CI_KAFKA_DOCKER_IMAGE", "apache/kafka:3.7.0")
      environment("GRAVITINO_CI_DORIS_DOCKER_IMAGE", "datastrato/gravitino-ci-doris:0.1.5")
      environment("GRAVITINO_CI_RANGER_DOCKER_IMAGE", "datastrato/gravitino-ci-ranger:0.1.1")

      copy {
        from("${project.rootDir}/dev/docker/trino/conf")
        into("build/trino-conf")
        fileMode = 0b111101101
      }

      // Get current project version
      val version = project.version.toString()
      println("Current project version: $version")

      // Check whether this module has already built
      val trinoConnectorBuildDir = project(":trino-connector").buildDir
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

    val init = project.extra.get("initIntegrationTest") as (Test) -> Unit
    init(this)
  }
}

tasks.clean {
  delete("metastore_db")
  delete("target")
  delete("derby.log")
}

tasks.register<JavaExec>("TrinoTest") {
  classpath = sourceSets["test"].runtimeClasspath
  mainClass.set("org.apache.gravitino.integration.test.trino.TrinoQueryTestTool")

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
