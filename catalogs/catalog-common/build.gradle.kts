/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("java")
  alias(libs.plugins.shadow)
  `maven-publish`
}

dependencies {
  implementation(project(":core"))
  implementation(project(":catalogs:catalog-hive"))
  implementation(project(":catalogs:catalog-lakehouse-iceberg"))
  implementation(project(":catalogs:catalog-jdbc-mysql"))
  implementation(project(":catalogs:catalog-jdbc-postgresql"))
}

tasks.test {
  useJUnitPlatform()
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  dependencies {
    exclude("org.*")
    exclude("javax.*")
  }

  exclude("**/package-info.class")
  exclude("**/*.properties")
  exclude("**/*.html")
  exclude("org/**")
  exclude("META-INF")
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
