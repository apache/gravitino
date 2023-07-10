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
        exclude("com.google.guava")
        exclude("com.google.protobuf")
        exclude("org.apache.thrift")
        exclude("org.apache.logging.log4j")
        exclude("org.slf4j")
    }

    // libs.hive2.metastore depends on it.
    implementation(libs.hadoop2.mapreduce.client.core) {
        exclude("*")
    }

    implementation(libs.hive2.exec) {
        artifact {
            classifier = "core"
        }
        exclude("com.google.guava")
        exclude("com.google.protobuf")
        exclude("org.apache.thrift")
        exclude("org.apache.zookeeper")
        exclude("org.apache.logging.log4j")
        exclude("org.pentaho", "pentaho-aggdesigner-algorithm")
        exclude("org.slf4j")
        exclude("com.fasterxml.jackson.core")
        exclude("com.fasterxml.jackson.datatype")
    }

    implementation(libs.thrift.libfb303) {
        exclude("org.slf4j")
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