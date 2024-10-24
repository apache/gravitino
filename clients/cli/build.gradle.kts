/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(libs.commons.cli.new)
  implementation(libs.guava)
  implementation(libs.slf4j.api)
  implementation(libs.slf4j.simple)
  implementation(project(":api"))
  implementation(project(":clients:client-java"))
  implementation(project(":common"))

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.build {
  dependsOn("javadoc")
}

tasks.clean {
  delete("target")
  delete("tmp")
}

tasks.jar {
  manifest {
    attributes["Main-Class"] = "org.apache.gravitino.cli.Main"
  }
  val dependencies = configurations
    .runtimeClasspath
    .get()
    .map(::zipTree)
  from(dependencies)
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
