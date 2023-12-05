/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.protobuf)
}

dependencies {
  implementation(libs.protobuf.java)
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
