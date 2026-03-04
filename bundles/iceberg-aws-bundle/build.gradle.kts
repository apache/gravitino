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

dependencies {
  // used by Gravitino credential vending
  implementation(libs.aws.policy) {
    exclude("*")
  }
  implementation(libs.iceberg.aws.bundle)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  dependencies {
    exclude(dependency("org.slf4j:slf4j-api"))
  }

  // Iceberg AWS bundle includes Log4j (before 1.10.1), so exclude to avoid conflicts
  // see https://github.com/apache/iceberg/pull/14225
  exclude("org/apache/log4j/**")
  exclude("org/apache/logging/log4j/**")
  exclude("log4j.properties")
  exclude("log4j2.xml")
  exclude("log4j2.component.properties")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
