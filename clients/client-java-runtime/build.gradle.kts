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
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

configurations.all {
  resolutionStrategy.eachDependency {
    if (requested.group == "org.apache.logging.log4j") {
      throw GradleException("Dependency 'org.apache.logging.log4j' is not allowed.")
    }
  }
}

dependencies {
  implementation(project(":clients:client-java"))
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  // Relocate dependencies to avoid conflicts
  relocate("com.google", "org.apache.gravitino.shaded.com.google")
  relocate("com.fasterxml", "org.apache.gravitino.shaded.com.fasterxml")
  relocate("org.apache.httpcomponents", "org.apache.gravitino.shaded.org.apache.httpcomponents")
  relocate("org.apache.commons", "org.apache.gravitino.shaded.org.apache.commons")
  relocate("org.antlr", "org.apache.gravitino.shaded.org.antlr")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
