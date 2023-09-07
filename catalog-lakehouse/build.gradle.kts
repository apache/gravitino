/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-lakehouse"

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
    implementation(libs.jackson.databind)
    implementation(libs.jackson.annotations)
    implementation(libs.jackson.datatype.jdk8)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.guava)
    implementation(libs.bundles.log4j)
    implementation(libs.bundles.jetty)
    implementation(libs.bundles.jersey)
    implementation(libs.bundles.iceberg)
    implementation(libs.substrait.java.core) {
      exclude("com.fasterxml.jackson.core")
      exclude("com.fasterxml.jackson.datatype")
      exclude("com.fasterxml.jackson.dataformat")
      exclude("com.google.protobuf")
      exclude("com.google.code.findbugs")
      exclude("org.slf4j")
    }

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation(libs.slf4j.jdk14)
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
        dependsOn(copyDepends)
        from("build/libs")
        into("${rootDir}/distribution/package/catalogs/lakehouse/libs")
    }
}
