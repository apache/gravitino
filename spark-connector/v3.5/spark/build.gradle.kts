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
import com.diffplug.gradle.spotless.SpotlessExtension

plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

val glueHiveJarsDir: String? = project(":spark-connector").extra["glueHiveJarsDir"] as String?

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark35.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4spark35.get()
val paimonVersion: String = libs.versions.paimon.get()
val kyuubiVersion: String = libs.versions.kyuubi4spark.get()
val scalaJava8CompatVersion: String = libs.versions.scala.java.compat.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()
val artifactName = "${rootProject.name}-spark-${sparkMajorVersion}_$scalaVersion"

// The connector's shared sources are compiled here rather than consumed as a jar, so that every
// supported Spark version builds them against its own API. `spark35` holds the one thing a Spark 4
// build cannot compile: the Paimon package, which has no paimon-spark-4.x artifact at the Paimon
// version this repository pins.
val sparkCommonDir = project(":spark-connector").projectDir.resolve("spark-common")

sourceSets {
  named("main") {
    java {
      setSrcDirs(
        listOf(
          "$sparkCommonDir/src/main/java",
          "$sparkCommonDir/src/main/spark35",
          "src/main/java"
        )
      )
    }
    resources {
      setSrcDirs(listOf("src/main/resources"))
    }
  }
  named("test") {
    java {
      setSrcDirs(
        listOf(
          "$sparkCommonDir/src/test/java",
          "src/test/java"
        )
      )
    }
    resources {
      // Only this module's test resources. spark-common's test resources were never packaged
      // either: the golden-file suites read spark-test.conf, test-sqls and data from the source
      // tree via GRAVITINO_ROOT_DIR, and its log4j2.properties would collide with this one.
      setSrcDirs(listOf("src/test/resources"))
    }
  }
}

if (hasProperty("excludePackagesForSparkConnector")) {
  @Suppress("UNCHECKED_CAST")
  val configureFunc = properties["excludePackagesForSparkConnector"] as? (Project) -> Unit
  configureFunc?.invoke(project)
}

// The source dirs above reach outside this project, and Spotless rejects cross-project targets.
// The shared tree is formatted by the :spark-connector project that owns it, so scope Spotless here
// to this module's own sources. Same approach as the trino-connector version modules.
plugins.withId("com.diffplug.spotless") {
  configure<SpotlessExtension> {
    java {
      target(project.fileTree("src") { include("**/*.java") })
    }
  }
}

dependencies {
  implementation(project(":catalogs:catalog-common")) {
    exclude("org.apache.logging.log4j")
  }
  implementation(libs.guava)
  implementation(libs.caffeine)

  compileOnly("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion") {
    exclude("com.fasterxml.jackson")
  }
  compileOnly("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
  compileOnly("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
  compileOnly("org.scala-lang.modules:scala-java8-compat_$scalaVersion:$scalaJava8CompatVersion")
  compileOnly(project(":clients:client-java-runtime", configuration = "shadow"))
  compileOnly("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  compileOnly(libs.aws.glue)
  if (scalaVersion == "2.12") {
    compileOnly("org.apache.paimon:paimon-spark-$sparkMajorVersion:$paimonVersion") {
      exclude("org.apache.spark")
    }
  }

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(project(":api")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":catalogs:catalog-glue")) {
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

  testImplementation(libs.awaitility)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.nimbus.jose.jwt)
  // Iceberg's GlueCatalog references several AWS SDK modules at runtime; must be on test classpath
  testImplementation(libs.aws.dynamodb)
  testImplementation(libs.aws.glue)
  testImplementation(libs.aws.kms)
  testImplementation(libs.aws.s3)
  testImplementation(libs.aws.sts)
  testImplementation(libs.hadoop3.aws)
  // hadoop-aws declares hadoop-client-api as provided; add it explicitly so S3AFileSystem can load
  // org.apache.hadoop.fs.impl.prefetch.PrefetchingStatistics (added in 3.3.5) at runtime.
  testImplementation(libs.hadoop3.client.api)
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
    if (glueHiveJarsDir != null) {
      dependsOn(":spark-connector:downloadGlueHiveJars")
      jvmArgs("-Dglue.hive-jars-dir=$glueHiveJarsDir")
    }
    dependsOn(tasks.jar)
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar")
    dependsOn(":catalogs:catalog-hive:jar")
    dependsOn(":iceberg:iceberg-rest-server:jar")
    dependsOn(":lance:lance-rest-server:jar")
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
