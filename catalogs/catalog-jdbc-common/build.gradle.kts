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