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

    implementation(libs.hive2.metastore)
    implementation(libs.hive2.exec) {
        exclude("org.slf4j")
        exclude("org.pentaho", "pentaho-aggdesigner-algorithm")
    }
    implementation(libs.guava)
    implementation(libs.substrait.java.core) {
        exclude("org.slf4j")
        exclude("com.fasterxml.jackson.core")
        exclude("com.fasterxml.jackson.datatype")
    }

    testImplementation(libs.slf4j.jdk14)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}