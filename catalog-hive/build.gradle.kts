/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
description = "hive catalog"

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
        exclude("com.google.code.findbugs", "jsr305")
        exclude("org.apache.logging.log4j")
        exclude("org.apache.curator")
        exclude("org.pentaho")
//        exclude("com.github.joshelser")
        exclude("org.slf4j")
    }

    implementation(libs.hadoop2.mapreduce.client.core) {
        exclude("*")
    }
    implementation(libs.hadoop2.common) {
        exclude("*")
    }

    implementation(libs.slf4j.api)
    implementation(libs.guava)

    testImplementation(libs.slf4j.jdk14)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}

task("copyDependencies", type = Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
}

tasks.named("build") {
    finalizedBy("copyDependencies")
}
