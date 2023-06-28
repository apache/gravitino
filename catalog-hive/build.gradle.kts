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

    implementation(libs.hive2.metastore)
    implementation(libs.hive2.exec) {
        exclude("org.slf4j")
        exclude("org.pentaho", "pentaho-aggdesigner-algorithm")
    }
    implementation(libs.guava)

    testImplementation(libs.slf4j.jdk14)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
}