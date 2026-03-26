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
description = "catalog-glue"

plugins {
    `maven-publish`
    id("java")
    id("idea")
}

dependencies {
    implementation(project(":api")) {
        exclude("*")
    }
    implementation(project(":catalogs:catalog-common")) {
        exclude("*")
    }
    implementation(project(":common")) {
        exclude("*")
    }
    implementation(project(":core")) {
        exclude("*")
    }

    implementation(libs.guava)
    implementation(libs.slf4j.api)
    implementation(libs.commons.lang3)

    implementation("software.amazon.awssdk:glue:2.20.0")
    implementation("software.amazon.awssdk:auth:2.20.0")
    implementation("software.amazon.awssdk:regions:2.20.0")

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.mockito.core)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
    register("runtimeJars", Copy::class) {
        from(configurations.runtimeClasspath)
        into("build/libs")
    }

    val copyCatalogLibs by registering(Copy::class) {
        dependsOn("jar", "runtimeJars")
        from("build/libs") {
            exclude("guava-*.jar")
            exclude("log4j-*.jar")
            exclude("slf4j-*.jar")
        }
        into("$rootDir/distribution/package/catalogs/glue/libs")
    }

    register("copyLibAndConfig", Copy::class) {
        dependsOn(copyCatalogLibs)
    }
}

tasks.test {
    val skipITs = project.hasProperty("skipITs")
    if (skipITs) {
        exclude("**/integration/test/**")
    } else {
        dependsOn(tasks.jar)
    }
}

tasks.withType<com.diffplug.gradle.spotless.SpotlessCheck>().configureEach {
    enabled = false
}