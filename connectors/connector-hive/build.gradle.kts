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
}

tasks.test {
    useJUnitPlatform()
}