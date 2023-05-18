plugins {
    id("java")
}

group = "com.datastrato.catalog.connectors.mysql"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":connectors:commons"))

    implementation("com.google.guava:guava:30.1.1-jre")
    implementation("org.apache.commons:commons-lang3:3.12.0")
    implementation("org.projectlombok:lombok:1.18.26")
    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("com.google.inject:guice:4.1.0")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.slf4j:slf4j-api:1.7.32")
    testImplementation("org.mockito:mockito-core:3.12.4")

    compileOnly("com.google.auto.service:auto-service:1.0-rc3")
    annotationProcessor("com.google.auto.service:auto-service:1.0-rc3")
}

tasks.test {
    useJUnitPlatform()
}