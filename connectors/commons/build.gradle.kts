plugins {
    id("java")
}

group = "com.datastrato.catalog.connectors.commons"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    annotationProcessor("org.projectlombok:lombok:1.18.20")

    implementation("org.projectlombok:lombok:1.18.20")
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    implementation("com.google.guava:guava:30.1.1-jre")
    implementation("com.google.inject:guice:4.1.0")
    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("org.apache.tomcat:tomcat-jdbc:10.1.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.5")
    implementation("io.substrait:core:0.9.0")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("junit:junit:4.13.1")
    testImplementation("org.mockito:mockito-core:3.12.4")
}

tasks.test {
    useJUnitPlatform()
}