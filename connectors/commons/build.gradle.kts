plugins {
    id("java")
}

group = "com.datastrato.unified_catalog.connectors.commons"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    annotationProcessor(libs.lombok)
    implementation(libs.lombok)
    implementation(libs.google.findbugs.jsr305)
    implementation(libs.guava)
    implementation(libs.google.inject.guice)
    implementation(libs.slf4j.api)
    implementation(libs.apache.tomcat.jdbc)
    implementation(libs.jackson.databind)
    implementation(libs.substrait.java.core)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.junit)
    testImplementation(libs.mockito.core)
}

tasks.test {
    useJUnitPlatform()
}