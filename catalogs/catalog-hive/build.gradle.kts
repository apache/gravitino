/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-hive"

plugins {
    `maven-publish`
    id("java")
    id("idea")
    id("com.diffplug.spotless")
}

dependencies {
    implementation(project(":api"))
    implementation(project(":core"))

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

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
    }

    implementation(libs.hadoop2.mapreduce.client.core) {
        exclude("*")
    }
    implementation(libs.hadoop2.common) {
        exclude("*")
    }

    implementation(libs.substrait.java.core) {
        exclude("org.slf4j")
        exclude("com.fasterxml.jackson.core")
        exclude("com.fasterxml.jackson.datatype")
    }

    implementation(libs.slf4j.api)
    implementation(libs.guava)

    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.mockito.core)
}

tasks {
    val copyDepends by registering(Copy::class) {
        from(configurations.runtimeClasspath)
        into("build/libs")
    }

    val copyCatalogLibs by registering(Copy::class) {
        dependsOn(copyDepends)
        from("build/libs")
        into("${rootDir}/distribution/package/catalogs/hive/libs")
    }

    val copyCatalogConfig by registering(Copy::class) {
        from("src/main/resources")
        into("${rootDir}/distribution/package/catalogs/hive/conf")

        include("hive.conf")
        include("hive-site.xml.template")

        include("**/*.template")
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
        dependsOn(copyCatalogConfig, copyCatalogLibs)
    }
}
