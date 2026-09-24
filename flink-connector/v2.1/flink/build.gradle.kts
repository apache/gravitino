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
}

repositories {
  mavenCentral()
}

// flink-common's sources are compiled directly into this module's own sourceSet (in addition
// to flink-common's own jar) so that they are re-checked against this module's own
// Flink/Iceberg/Paimon dependency versions (this matters once versions diverge in incompatible
// ways, e.g. Flink 2.x removing APIs that are still present in the 1.x line).
val commonProject = project(":flink-connector:flink-common")

sourceSets {
  main {
    java.srcDir(commonProject.file("src/main/java"))
  }
  test {
    java.srcDir(commonProject.file("src/test/java"))
    resources.srcDir(commonProject.file("src/test/resources"))
  }
}

// Spotless can't format files outside the project dir; flink-common's own sources are linted
// by the flink-common project itself, so restrict this project's spotless target to its own.
plugins.withType<com.diffplug.gradle.spotless.SpotlessPlugin>().configureEach {
  configure<com.diffplug.gradle.spotless.SpotlessExtension> {
    java {
      target("src/**/*.java")
    }
  }
}

val flinkVersion: String = libs.versions.flink21.get()
val flinkMajorVersion: String = flinkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4flink21.get()
val paimonVersion: String = libs.versions.paimon4flink21.get()
// Flink 2.x removed the Scala APIs entirely, so unlike the 1.x modules there is no scala suffix.
val artifactName = "${rootProject.name}-flink-$flinkMajorVersion"

dependencies {
  constraints {
    // Force upgrade for outdated transitive libthrift pulled by Hive Metastore
    compileOnly(libs.thrift)
    testImplementation(libs.thrift)
    // flink-connector-jdbc-core transitively pulls openlineage-sql-java:1.32.0, whose bundled,
    // unrelocated org.apache.commons.lang3.SystemProperties predates the getUserName(String)
    // overload and shadows the real commons-lang3 on the classpath (NoSuchMethodError). This
    // can't simply be excluded: the connector's lineage extraction actually calls into it at
    // runtime (JdbcSource.getLineageVertex). Force this newer release instead, which ships an
    // up-to-date bundled copy.
    compileOnly(libs.openlineageSqlJava21)
    testImplementation(libs.openlineageSqlJava21)
  }

  // Dependencies needed to compile flink-common's sources (mirrors flink-common/build.gradle.kts).
  implementation(project(":catalogs:catalog-common")) {
    exclude("org.apache.logging.log4j")
  }
  implementation(libs.guava)
  implementation(libs.commons.lang3)

  compileOnly(project(":clients:client-java-runtime", configuration = "shadow"))
  compileOnly("org.apache.iceberg:iceberg-flink-runtime-$flinkMajorVersion:$icebergVersion")
  // flink-connector-hive has not published a build targeting the Flink 2.x line yet, so this
  // module has no concrete Hive catalog implementation and does not register one via SPI (see
  // META-INF/services/org.apache.flink.table.factories.Factory). This dependency is still needed
  // to compile flink-common's shared, version-agnostic Hive classes (GravitinoHiveCatalog,
  // GravitinoHiveCatalogFactory, ...), which reference Flink's own o.a.f.table.catalog.hive.*
  // types; it reuses the newest available release (built against 1.20).
  compileOnly(libs.flinkHive21)
  compileOnly("org.apache.flink:flink-table-common:$flinkVersion")
  compileOnly("org.apache.flink:flink-table-api-java:$flinkVersion")
  compileOnly("org.apache.paimon:paimon-flink-$flinkMajorVersion:$paimonVersion")
  compileOnly(libs.flinkjdbc21.core)
  compileOnly(libs.flinkjdbc21.mysql)
  compileOnly(libs.flinkjdbc21.postgres)
  compileOnly(libs.hive2.common) {
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.apache.logging.log4j")
  }
  compileOnly(libs.hive2.exec) {
    artifact {
      classifier = "core"
    }
    exclude("com.fasterxml.jackson.core")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.protobuf")
    exclude("org.apache.avro")
    exclude("org.apache.calcite")
    exclude("org.apache.calcite.avatica")
    exclude("org.apache.curator")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.openjdk.jol")
    exclude("org.pentaho")
    exclude("org.slf4j")
  }

  testImplementation(project(":api"))
  testImplementation(project(":catalogs:catalog-jdbc-common")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(libs.awaitility)
  testImplementation(libs.flinkjdbc21.core)
  testImplementation(libs.flinkjdbc21.mysql)
  testImplementation(libs.flinkjdbc21.postgres)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.metrics.core)
  testImplementation(libs.minikdc)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testImplementation(libs.testcontainers.mysql)

  testImplementation("org.apache.iceberg:iceberg-core:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-hive-metastore:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-flink-runtime-$flinkMajorVersion:$icebergVersion")
  testImplementation(libs.flinkHive21)
  testImplementation("org.apache.flink:flink-table-common:$flinkVersion")
  testImplementation("org.apache.flink:flink-table-api-java:$flinkVersion")
  testImplementation("org.apache.flink:flink-sql-gateway:$flinkVersion")
  testImplementation("org.apache.paimon:paimon-flink-$flinkMajorVersion:$paimonVersion")

  testImplementation(libs.hive2.exec) {
    artifact {
      classifier = "core"
    }
    exclude("com.fasterxml.jackson.core")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.protobuf")
    exclude("org.apache.avro")
    exclude("org.apache.calcite")
    exclude("org.apache.calcite.avatica")
    exclude("org.apache.curator")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.openjdk.jol")
    exclude("org.pentaho")
    exclude("org.slf4j")
  }

  testImplementation(libs.hadoop3.common) {
    exclude("*")
  }
  testImplementation(libs.hadoop3.hdfs) {
    exclude("com.sun.jersey")
    exclude("commons-cli", "commons-cli")
    exclude("commons-io", "commons-io")
    exclude("commons-codec", "commons-codec")
    exclude("commons-logging", "commons-logging")
    exclude("javax.servlet", "servlet-api")
    exclude("org.mortbay.jetty")
  }
  testImplementation(libs.hadoop3.hdfs.client)
  testImplementation(libs.hadoop3.mapreduce.client.core) {
    exclude("*")
  }
  // Hadoop 3.x runtime requirements (stripped by exclude("*") above)
  testImplementation(libs.hadoop3.shaded.guava)
  testImplementation(libs.hadoop3.shaded.protobuf)
  testImplementation(libs.commons.configuration2)
  testImplementation(libs.re2j)
  testImplementation(libs.hive2.common) {
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
    exclude("io.dropwizard.metrics")
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
  testImplementation("org.apache.flink:flink-table-api-bridge-base:$flinkVersion") {
    exclude("commons-cli", "commons-cli")
    exclude("commons-io", "commons-io")
    exclude("com.google.code.findbugs", "jsr305")
  }
  // Flink 2.x replaced the raw flink-table-planner dependency with flink-table-planner-loader,
  // which loads the planner through an isolated classloader.
  testImplementation("org.apache.flink:flink-table-planner-loader:$flinkVersion")
  // flink-table-planner-loader does not transitively pull in flink-table-runtime the way
  // flink-table-planner_2.12 used to on the 1.x line; without it, the SQL Gateway REST client's
  // shaded jayway JsonPath classes are missing at runtime (NoClassDefFoundError on
  // org.apache.flink.shaded.com.jayway.jsonpath.spi.mapper.MappingProvider).
  testImplementation("org.apache.flink:flink-table-runtime:$flinkVersion")
  testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
    dependsOn(":catalogs:catalog-hive:jar")
    dependsOn(":catalogs:catalog-hive:runtimeJars")
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar")
    dependsOn(":catalogs:catalog-lakehouse-iceberg:runtimeJars")
    dependsOn(":iceberg:iceberg-rest-server:jar")
    dependsOn(":catalogs:catalog-lakehouse-paimon:jar")
    dependsOn(":catalogs:catalog-lakehouse-paimon:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-mysql:jar")
    dependsOn(":catalogs:catalog-jdbc-mysql:runtimeJars")
    dependsOn(":catalogs:catalog-jdbc-postgresql:jar")
    dependsOn(":catalogs:catalog-jdbc-postgresql:runtimeJars")
  }
}

tasks.withType<Jar> {
  archiveBaseName.set(artifactName)
}

publishing {
  publications {
    withType<MavenPublication>().configureEach {
      artifactId = artifactName
    }
  }
}

tasks.named<Jar>("sourcesJar") {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
