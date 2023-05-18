plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless")
}

dependencies {
  implementation(project(":schema"));
  implementation(libs.protobuf.java.util) {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Andriod, which we don't want (and breaks multimaps).")
  }
  implementation(libs.substrait.java.core)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.guava)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}