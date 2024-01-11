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

group = "org.example"
version = "0.4.0-SNAPSHOT"

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

  // 排除不需要的类和文件
  exclude("**/package-info.class")
  exclude("**/*.properties")
  exclude("**/*.html")
  exclude("org/**")
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
  exclude("**/*.ddl")

  minimize() // 移除所有未使用的类
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
