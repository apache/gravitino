/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-hive"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":core"))

  compileOnly(libs.immutables.value)
  compileOnly(libs.lombok)

  annotationProcessor(libs.immutables.value)
  annotationProcessor(libs.lombok)

  implementation(libs.hive2.metastore) {
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
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("org.apache.hbase")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.openjdk.jol")
    exclude("org.slf4j")
  }

  implementation(libs.hive2.exec) {
    artifact {
      classifier = "core"
    }
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

  implementation(libs.caffeine)
  implementation(libs.guava)
  implementation(libs.hadoop2.common) {
    exclude("*")
  }
  implementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  implementation(libs.slf4j.api)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    // Why should we rename the jar files? Because the directory `build/libs` is the output directory of
    // the task `build` and `copyDepends`. Task `shadowJar` of project `bundled-catalog` depends on the output
    // of task `build` and mistakenly thinks that it depends on the task `copyDepends`, and errors occur.
    // The same goes for `catalog-lakehouse-iceberg`, `catalog-jdbc-mysql` and `catalog-jdbc-postgresql`.
    into("build/libs_all")
  }

  val copyCatalogLibs by registering(Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs_all", "build/libs")
    into("$rootDir/distribution/package/catalogs/hive/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/hive/conf")

    include("hive.conf")
    include("hive-site.xml.template")

    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }

    exclude { details ->
      details.file.isDirectory()
    }
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyCatalogConfig, copyCatalogLibs)
  }
}
