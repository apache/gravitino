plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
}

dependencies {
  implementation(project(":schema"));
  implementation("com.google.protobuf:protobuf-java-util:${project.property("protoc.version")}") {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Andriod, which we don't want (and breaks multimaps).")
  }
  implementation("io.substrait:core:${project.property("substrait.version")}")
  implementation(
    "com.fasterxml.jackson.core:jackson-databind:${project.property("jackson.version")}")
  implementation(
    "com.fasterxml.jackson.core:jackson-annotations:${project.property("jackson.version")}")
  implementation(
    "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:${project.property("jackson.version")}")
  implementation("com.google.guava:guava:${project.property("guava.version")}")

  compileOnly("org.projectlombok:lombok:${project.property("lombok.version")}")
  annotationProcessor("org.projectlombok:lombok:${project.property("lombok.version")}")
  testCompileOnly("org.projectlombok:lombok:${project.property("lombok.version")}")
  testAnnotationProcessor(
    "org.projectlombok:lombok:${project.property("lombok.version")}")
  testImplementation("org.junit.jupiter:junit-jupiter-api:${project.property("junit5.version")}")
  testImplementation("org.junit.jupiter:junit-jupiter-params:${project.property("junit5.version")}")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(8))
    withJavadocJar()
    withSourcesJar()
  }
}