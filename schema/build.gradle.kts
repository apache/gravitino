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
}

sourceSets {
  main {
    proto.srcDir("schema/src/main/proto")
    resources.srcDir("schema/src/main/resources")
  }
}

protobuf {
  protoc { artifact = "com.google.protobuf:protoc:${libs.versions.protoc.get()}"}
}
