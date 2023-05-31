description = "hive catalog"

plugins {
    `maven-publish`
    id("java")
    id("idea")
    id("com.diffplug.spotless")
}

group = "com.datastrato.graviton.catalog.hive"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":core"))

    implementation(libs.substrait.java.core)
    implementation(libs.hive2.metastore)
    implementation(libs.hive2.exec) {
        exclude("org.pentaho", "pentaho-aggdesigner-algorithm")
    }
    implementation(libs.airlift.units)
    implementation(libs.airlift.log)
    implementation(libs.guava)
    implementation(libs.hadoop2.common)
    implementation(libs.hadoop2.mapreduce.client.core)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.junit.jupiter.params)
    testImplementation(libs.guava)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}