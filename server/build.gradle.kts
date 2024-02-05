/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import java.text.SimpleDateFormat
import java.util.Date

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":server-common"))
  implementation(project(":core"))
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.metrics.jersey2)

  // As of Java 9 or newer, the javax.activation package (needed by the jetty server) is no longer part of the JDK. It was removed because it was part of the
  // JavaBeans Activation Framework (JAF) which has been removed from Java SE. So we need to add it as a dependency. For more,
  // please see: https://stackoverflow.com/questions/46493613/what-is-the-replacement-for-javax-activation-package-in-java-9
  implementation(libs.sun.activation)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.commons.io)
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }

  testRuntimeOnly(libs.junit.jupiter.engine)
}

fun getGitCommitId(): String {
  var gitCommitId: String
  try {
    val gitFolder = rootDir.path + "/.git/"
    val head = File(gitFolder + "HEAD").readText().split(":")
    val isCommit = head.size == 1
    gitCommitId = if (isCommit) {
      head[0].trim()
    } else {
      val refHead = File(gitFolder + head[1].trim())
      refHead.readText().trim()
    }
  } catch (e: Exception) {
    println("WARN: Unable to get Git commit id : ${e.message}")
    gitCommitId = ""
  }
  return gitCommitId
}

val propertiesFile = "src/main/resources/project.properties"
fun writeProjectPropertiesFile() {
  val propertiesFile = file(propertiesFile)
  if (propertiesFile.exists()) {
    propertiesFile.delete()
  }

  val dateFormat = SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

  val compileDate = dateFormat.format(Date())
  val projectVersion = project.version.toString()
  val commitId = getGitCommitId()

  propertiesFile.parentFile.mkdirs()
  propertiesFile.createNewFile()
  propertiesFile.writer().use { writer ->
    writer.write(
      "#\n" +
        "# Copyright 2023 Datastrato Pvt Ltd.\n" +
        "# This software is licensed under the Apache License version 2.\n" +
        "#\n"
    )
    writer.write("project.version=$projectVersion\n")
    writer.write("compile.date=$compileDate\n")
    writer.write("git.commit.id=$commitId\n")
  }
}

tasks {
  jar {
    doFirst() {
      writeProjectPropertiesFile()
      val file = file(propertiesFile)
      if (!file.exists()) {
        throw GradleException("$propertiesFile file not generated!")
      }
    }
  }
  test {
    environment("GRAVITINO_HOME", rootDir.path)
    environment("GRAVITINO_TEST", "true")
  }
  clean {
    delete("$propertiesFile")
  }
}
