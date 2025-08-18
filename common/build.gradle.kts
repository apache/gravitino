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

import java.text.SimpleDateFormat
import java.util.Date

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))

  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jackson.databind)
  implementation(libs.jakarta.validation.api)
  implementation(libs.slf4j.api)

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
    val gitPath = File(rootDir.path, ".git")
    val gitFolder = if (gitPath.isDirectory) {
      gitPath
    } else {
      val content = gitPath.readText().trim()
      File(rootDir.path, content.substringAfter("gitdir: ").trim())
    }

    val head = File(gitFolder, "HEAD").readText().split(":")
    val isCommit = head.size == 1
    gitCommitId = if (isCommit) {
      head[0].trim()
    } else {
      val refHead = File(gitFolder, head[1].trim())
      refHead.readText().trim()
    }
  } catch (e: Exception) {
    println("WARN: Unable to get Git commit id : ${e.message}")
    gitCommitId = ""
  }
  return gitCommitId
}

val propertiesFile = "src/main/resources/gravitino-build-info.properties"
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
        "# Licensed to the Apache Software Foundation (ASF) under one\n" +
        "# or more contributor license agreements.  See the NOTICE file\n" +
        "# distributed with this work for additional information\n" +
        "# regarding copyright ownership.  The ASF licenses this file\n" +
        "# to you under the Apache License, Version 2.0 (the\n" +
        "# \"License\"); you may not use this file except in compliance\n" +
        "# with the License.  You may obtain a copy of the License at\n" +
        "#\n" +
        "#  http://www.apache.org/licenses/LICENSE-2.0\n" +
        "#\n" +
        "# Unless required by applicable law or agreed to in writing,\n" +
        "# software distributed under the License is distributed on an\n" +
        "# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n" +
        "# KIND, either express or implied.  See the License for the\n" +
        "# specific language governing permissions and limitations\n" +
        "# under the License.\n" +
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
      if (!file(propertiesFile).exists()) {
        throw GradleException("Failed to generate $propertiesFile.")
      }
    }

    from("src/main/resources") {
      include("gravitino-build-info.properties").duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
  }

  clean {
    delete("$propertiesFile")
  }
}
