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
        exclude("org.apache.hive", "hive-serde").because("It will imports org.apache.hadoop:hadoop-yarn-server-resourcemanager")
        exclude("org.apache.hive", "hive-shims").because("It will imports org.apache.hadoop:hadoop-yarn-server-resourcemanager")
        exclude("org.apache.hive.shims", "hive-shims-0.23").because("It will imports org.apache.hadoop:hadoop-yarn-server-resourcemanager")
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

    // libs.hive2.metastore depends on it.
    implementation(libs.hive2.shims023) {
        exclude("*")
    }
    implementation(libs.commons.collections)?.because("libs.hive2.shims023 depends on it.")
    implementation(libs.commons.io)?.because("libs.hive2.shims023 depends on it.")

    implementation(libs.hive2.exec) {
        artifact {
            classifier = "core"
        }
        exclude("com.google.guava")
        exclude("com.google.protobuf")
        exclude("org.apache.thrift")
        exclude("org.apache.hive", "hive-llap-tez").because("It will imports org.apache.hadoop:hadoop-yarn-server-resourcemanager")
        exclude("org.apache.hive", "hive-shims").because("It will imports org.apache.hadoop:hadoop-yarn-server-resourcemanager")
        exclude("org.apache.hive", "hive-vector-code-gen").because("This depends is not used in HiveCatalog.")
        exclude("org.apache.zookeeper")
        exclude("org.apache.logging.log4j")
        exclude("org.pentaho", "pentaho-aggdesigner-algorithm")
        exclude("org.slf4j")
        exclude("com.fasterxml.jackson.core")
        exclude("com.fasterxml.jackson.datatype")
    }

    implementation(libs.hive2.shims.common) {
        exclude("*")
    }

    implementation(libs.hadoop2.mapreduce.client.core) {
        exclude("*")
    }

    implementation(libs.hive2.serde) {
        exclude("*")
    }

    implementation(libs.hive2.common) {
        exclude("*")
    }
    implementation(libs.commons.lang3)

    implementation(libs.hadoop2.common) {
        exclude("*")
    }
    implementation(libs.javax.servlet.api)?.because("libs.hadoop2.common depends on it.")

    implementation(libs.hadoop2.auth) {
        exclude("*")
    }

    implementation(libs.commons.configuration) {
        exclude("*")
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