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

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark35.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4connector.get()
val paimonVersion: String = libs.versions.paimon.get()
val kyuubiVersion: String = libs.versions.kyuubi4spark.get()
val scalaJava8CompatVersion: String = libs.versions.scala.java.compat.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()
val artifactName = "${rootProject.name}-spark-${sparkMajorVersion}_$scalaVersion"

if (hasProperty("excludePackagesForSparkConnector")) {
  val configureFunc = properties["excludePackagesForSparkConnector"] as? (Project) -> Unit
  configureFunc?.invoke(project)
}

dependencies {
  implementation(project(":spark-connector:spark-3.4"))
  implementation(project(":spark-connector:spark-common"))
  compileOnly("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion") {
    exclude("com.fasterxml.jackson")
  }
  compileOnly(project(":clients:client-java-runtime", configuration = "shadow"))
  compileOnly("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  if (scalaVersion == "2.12") {
    compileOnly("org.apache.paimon:paimon-spark-$sparkMajorVersion:$paimonVersion") {
      exclude("org.apache.spark")
    }
  }

  testImplementation(project(":api")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":catalogs:catalog-jdbc-common")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":catalogs:hive-metastore-common")) {
    exclude("*")
  }
  testImplementation(project(":clients:client-java")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":core")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":common")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":integration-test-common", "testArtifacts")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":server")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":server-common")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":spark-connector:spark-common", "testArtifacts")) {
    exclude("com.fasterxml.jackson")
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }

  testImplementation(libs.hive2.common) {
    exclude("com.sun.jersey")
    exclude("org.apache.curator")
    exclude("org.apache.logging.log4j")
    // use hadoop from Spark
    exclude("org.apache.hadoop")
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
    exclude("com.sun.jersey")
    exclude("io.dropwizard.metrics")
    exclude("javax.transaction", "transaction-api")
    exclude("org.apache.avro")
    exclude("org.apache.curator")
    exclude("org.apache.hbase")
    exclude("org.apache.hadoop")
    exclude("org.apache.hive", "hive-common")
    exclude("org.apache.hive", "hive-shims")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.slf4j")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)

  // org.apache.iceberg.rest.RESTSerializers#registerAll(ObjectMapper) has different method signature for iceberg-core and iceberg-spark-runtime package, we must make sure iceberg-core is in front to start up MiniGravitino server.
  testImplementation("org.apache.iceberg:iceberg-core:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-hive-metastore:$icebergVersion")
  if (scalaVersion == "2.12") {
    testImplementation("org.apache.paimon:paimon-spark-$sparkMajorVersion:$paimonVersion") {
      exclude("org.apache.spark")
    }
  }
  testImplementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  // include spark-sql,spark-catalyst,hive-common,hdfs-client
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion") {
    // conflict with Gravitino server jersey
    exclude("org.glassfish.jersey.core")
    exclude("org.glassfish.jersey.containers")
    exclude("org.glassfish.jersey.inject")
    exclude("com.sun.jersey")
    exclude("com.fasterxml.jackson")
    exclude("com.fasterxml.jackson.core")
  }
  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")
  testImplementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
  testImplementation("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  val enableSparkSQLITs = project.hasProperty("enableSparkSQLITs")
  if (!enableSparkSQLITs) {
    exclude("**/integration/test/sql/**")
  }
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar")
    dependsOn(":catalogs:catalog-hive:jar")
    dependsOn(":iceberg:iceberg-rest-server:jar")
    dependsOn(":catalogs:catalog-lakehouse-paimon:jar")
    dependsOn(":catalogs:catalog-jdbc-mysql:jar")
    dependsOn(":catalogs:catalog-jdbc-postgresql:jar")
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

tasks.clean {
  delete("derby.log")
  delete("metastore_db")
  delete("spark-warehouse")
}

tasks.named<Jar>("sourcesJar") {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
