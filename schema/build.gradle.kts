import com.google.protobuf.gradle.*

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.google.protobuf") version "0.9.2"
  id("com.diffplug.spotless") version "6.11.0"
}

dependencies {
  implementation("com.google.protobuf:protobuf-java:${project.property("protoc.version")}")
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(8))
    withJavadocJar()
    withSourcesJar()
  }
}

sourceSets {
  main {
    proto.srcDir("schema/src/main/proto")
    resources.srcDir("schema/src/main/resources")
  }
}

protobuf {
  protoc { artifact = "com.google.protobuf:protoc:${project.property("protoc.version")}" }
}
