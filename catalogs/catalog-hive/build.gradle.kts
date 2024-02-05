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

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)

  compileOnly(libs.immutables.value)
  annotationProcessor(libs.immutables.value)

  implementation(libs.hive2.metastore) {
    exclude("org.apache.hbase")
    exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    exclude("co.cask.tephra")
    exclude("org.apache.avro")
    exclude("org.apache.zookeeper")
    exclude("org.apache.logging.log4j")
    exclude("com.google.code.findbugs", "sr305")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("com.tdunning", "json")
    exclude("javax.transaction", "transaction-api")
    exclude("com.zaxxer", "HikariCP")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("org.apache.curator")
    exclude("com.github.joshelser")
    exclude("io.dropwizard.metricss")
    exclude("org.slf4j")
    exclude("org.openjdk.jol")
  }

  implementation(libs.hive2.exec) {
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
    exclude("org.openjdk.jol")
  }

  implementation(libs.hadoop2.mapreduce.client.core) {
    exclude("*")
  }
  implementation(libs.hadoop2.common) {
    exclude("*")
  }

  implementation(libs.slf4j.api)
  implementation(libs.guava)
  implementation(libs.caffeine)

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
