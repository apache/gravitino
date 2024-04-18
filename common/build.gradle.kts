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

  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jackson.databind)
  implementation(libs.protobuf.java)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)

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
val writeProjectPropertiesFile = tasks.register("writeProjectPropertiesFile") {
  doFirst() {
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
}

tasks {
  jar {
    dependsOn(writeProjectPropertiesFile)
    doFirst() {
      if (!file(propertiesFile).exists()) {
        throw GradleException("$propertiesFile file not generated!")
      }
    }
  }
  clean {
    delete("$propertiesFile")
  }
}
