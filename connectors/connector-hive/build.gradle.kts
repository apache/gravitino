description = "connectors hive"

plugins {
    id("java")
}

group = "com.datastrato.graviton.connectors.hive"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":connectors:connector-core"))

    implementation(libs.hive.metastrore.v230)
    implementation(libs.hive.exec.v230)
    implementation(libs.airlift.units)
    implementation(libs.airlift.log)
    implementation(libs.guava)
    implementation(libs.hadoop2.common)
    implementation(libs.hadoop2.mapreduce.client.core)

    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.junit.jupiter.params)
    testImplementation(libs.guava)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}