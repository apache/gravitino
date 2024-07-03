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
  alias(libs.plugins.shadow)
}

dependencies {
  implementation(project(":catalogs:catalog-hive"))
  implementation(project(":catalogs:catalog-jdbc-common"))
  implementation(project(":catalogs:catalog-jdbc-mysql"))
  implementation(project(":catalogs:catalog-jdbc-postgresql"))
  implementation(project(":catalogs:catalog-lakehouse-iceberg"))
  implementation(project(":catalogs:catalog-lakehouse-paimon"))
  implementation(project(":core"))
  implementation(libs.slf4j.api)
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.compileClasspath.get())
  archiveClassifier.set("")

  dependencies {
    exclude("javax.*")
    exclude("org.*")
  }

  exclude("**/package-info.class")
  exclude("**/*.properties")
  exclude("**/*.html")
  exclude("org/**")
  exclude("META-INF")
  exclude("META-INF/**")
  exclude("module-info.class")
  exclude("com/google/**")
  exclude("com/fasterxml/**")
  exclude("javax/**")
  exclude("schema/**")
  exclude("fr/**")
  exclude("google/**")
  exclude("groovy/**")
  exclude("images/**")
  exclude("**/*.conf")
  exclude("**/*.so")
  exclude("**/*.sxd")
  exclude("**/*.xsd")
  exclude("*.ddl")
  exclude("**/*.txt")
  exclude("**/*.md")
  exclude("**/*.dtd")
  exclude("**/*.thrift")
  exclude("**/*.jdo")
  exclude("**/LICENSE")
  exclude("**/*.MF")
  exclude("**/*.xml")
  exclude("*.proto")
  exclude("*.template")
  exclude("webapps")
  exclude("license/*")
  exclude("*.xml")
  exclude("*.css")
  exclude("*.jnilib")
  exclude("*.dll")
  exclude("*.jocl")
  exclude("NOTICE")

  minimize()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-jdbc-postgresql:runtimeJars")
  dependsOn(":catalogs:catalog-lakehouse-iceberg:runtimeJars")
  dependsOn(":catalogs:catalog-lakehouse-paimon:runtimeJars")
  dependsOn(":catalogs:catalog-jdbc-mysql:runtimeJars")
  dependsOn(":catalogs:catalog-hive:runtimeJars")
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
}
