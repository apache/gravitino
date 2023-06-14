import com.google.protobuf.gradle.*

plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.protobuf)
  alias(libs.plugins.spotless)
}

dependencies {
  implementation(libs.protobuf.java)
  implementation(libs.substrait.java.core) {
    exclude("org.slf4j")
    exclude("com.fasterxml.jackson.core")
    exclude("com.fasterxml.jackson.datatype")
  }
}

sourceSets {
  main {
    proto.srcDir("meta/src/main/proto")
    resources.srcDir("meta/src/main/resources")
  }
}

protobuf {
  protoc { artifact = "com.google.protobuf:protoc:${libs.versions.protoc.get()}"}
}
