/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-jdbc-common"

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
    implementation(libs.commons.lang3)
    implementation(libs.commons.collections4)
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
}

tasks {
    val copyDepends by registering(Copy::class) {
        from(configurations.runtimeClasspath)
        into("build/libs")
    }
    val copyCatalogLibs by registering(Copy::class) {
        dependsOn(copyDepends, "build")
        from("build/libs")
        into("${rootDir}/distribution/package/catalogs/jdbc-common/libs")
    }

    val copyCatalogConfig by registering(Copy::class) {
        from("src/main/resources")
        into("${rootDir}/distribution/package/catalogs/jdbc-common/conf")

        include("jdbc.properties")
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
