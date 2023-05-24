plugins {
    id("java")
}

group = "com.datastrato.unified_catalog.connectors.mysql"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":connectors:commons"))

    compileOnly(libs.google.auto.service)
    annotationProcessor(libs.google.auto.service)
    implementation(libs.guava)
    implementation(libs.apache.commons.lang3)
    implementation(libs.lombok)
    implementation(libs.slf4j.api)
    implementation(libs.google.inject.guice)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.slf4j.api)
    testImplementation(libs.mockito.core)
}

tasks.test {
    useJUnitPlatform()
}