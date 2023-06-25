description = "connectors core"

plugins {
    id("java")
}

group = "com.datastrato.graviton.connectors.connector-core"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":core"))
}
