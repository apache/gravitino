/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-lakehouse-iceberg"

plugins {
    `maven-publish`
    id("java")
    id("idea")
    id("com.diffplug.spotless")
}

dependencies {
    implementation(project(":common"))
    implementation(project(":core"))
    implementation(project(":api"))
    implementation(project(":server-common"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.annotations)
    implementation(libs.jackson.datatype.jdk8)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.guava)
    implementation(libs.commons.lang3)
    implementation(libs.bundles.log4j)
    implementation(libs.bundles.jetty)
    implementation(libs.bundles.jersey)
    implementation(libs.bundles.iceberg)
    implementation(libs.commons.lang3)
    implementation(libs.commons.io)
    implementation(libs.commons.collections4)
    implementation(libs.substrait.java.core) {
      exclude("com.fasterxml.jackson.core")
      exclude("com.fasterxml.jackson.datatype")
      exclude("com.fasterxml.jackson.dataformat")
      exclude("com.google.protobuf")
      exclude("com.google.code.findbugs")
      exclude("org.slf4j")
    }
    implementation(libs.iceberg.hive.metastore)
    implementation(libs.sqlite.jdbc)

    implementation(libs.hive2.metastore) {
        exclude("org.apache.avro", "avro")
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("org.pentaho") // missing dependency
        exclude("org.apache.hbase")
        exclude("org.apache.logging.log4j")
        exclude("co.cask.tephra")
        exclude("com.google.code.findbugs", "jsr305")
        exclude("org.eclipse.jetty.aggregate","jetty-all")
        exclude("org.eclipse.jetty.orbit","javax.servlet")
        exclude("org.apache.parquet", "parquet-hadoop-bundle")
        exclude("com.tdunning", "json")
        exclude("javax.transaction", "transaction-api")
        exclude("com.zaxxer","HikariCP")
        exclude("org.apache.zookeeper")
        exclude("org.apache.hadoop", "hadoop-yarn-server-common")
        exclude("org.apache.hadoop", "hadoop-yarn-server-applicationhistoryservice")
        exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")
        exclude("org.apache.hadoop", "hadoop-yarn-api")
        exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    }

    implementation(libs.hadoop2.hdfs)
    implementation(libs.hadoop2.common)
    implementation(libs.hadoop2.mapreduce.client.core)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.junit.jupiter.params)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.mockito.core)
    testImplementation(libs.jersey.test.framework.core) {
      exclude(group = "org.junit.jupiter")
    }
    testImplementation(libs.jersey.test.framework.provider.jetty) {
      exclude(group = "org.junit.jupiter")
    }
}

tasks {
    val copyDepends by registering(Copy::class) {
        from(configurations.runtimeClasspath)
        into("build/libs")
    }
    val copyCatalogLibs by registering(Copy::class) {
        dependsOn(copyDepends, "build")
        from("build/libs")
        into("${rootDir}/distribution/package/catalogs/lakehouse-iceberg/libs")
    }

    val copyCatalogConfig by registering(Copy::class) {
        from("src/main/resources")
        into("${rootDir}/distribution/package/catalogs/lakehouse-iceberg/conf")

        include("lakehouse-iceberg.conf")
        include("hive-site.xml.template")
        include("hdfs-site.xml.template")

        rename { original -> if (original.endsWith(".template")) {
            original.replace(".template", "")
        } else {
            original
        }}

        exclude { details ->
            details.file.isDirectory()
        }
    }

    val copyLibAndConfig by registering(Copy::class) {
        dependsOn(copyCatalogLibs, copyCatalogConfig)
    }
}
