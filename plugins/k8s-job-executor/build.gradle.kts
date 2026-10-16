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
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("java")
  id("idea")
  alias(libs.plugins.shadow)
}

dependencies {
  annotationProcessor(libs.lombok)

  // Provided by the Gravitino server at runtime, never bundled into the shaded jar.
  compileOnly(project(":api"))
  compileOnly(project(":common"))
  compileOnly(project(":core"))
  compileOnly(libs.commons.lang3)
  compileOnly(libs.guava)
  compileOnly(libs.lombok)
  compileOnly(libs.slf4j.api)

  // Bundled and relocated, as fabric8 needs a newer Jackson than the Gravitino server ships.
  implementation(libs.fabric8.kubernetes.client) {
    exclude(group = "io.fabric8", module = "kubernetes-httpclient-okhttp")
    exclude(group = "io.fabric8", module = "kubernetes-httpclient-vertx")
  }
  implementation(libs.fabric8.kubernetes.httpclient.jdk)

  testImplementation(project(":api"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(libs.commons.lang3)
  testImplementation(libs.fabric8.kubernetes.server.mock)
  testImplementation(libs.guava)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")
  // fabric8 loads its HTTP client and resource handlers through ServiceLoader.
  mergeServiceFiles()

  exclude("module-info.class")
  exclude("META-INF/versions/**")
  // Leftovers of the fabric8 build, of no use at runtime.
  exclude("META-INF/jandex.idx")
  exclude("manifest.vm")

  dependencies {
    exclude(dependency("org.slf4j:.*"))
  }

  // Use a prefix of its own: the built-in jobs jar already relocates Jackson under
  // org.apache.gravitino.shaded.
  relocate("com.fasterxml.jackson", "org.apache.gravitino.k8s.shaded.com.fasterxml.jackson")
  relocate("org.snakeyaml.engine", "org.apache.gravitino.k8s.shaded.org.snakeyaml.engine")
  relocate("org.yaml.snakeyaml", "org.apache.gravitino.k8s.shaded.org.yaml.snakeyaml")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
