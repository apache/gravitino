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
import java.util.concurrent.TimeUnit

plugins {
  `maven-publish`
  `java-test-fixtures`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))

  implementation(libs.commons.collections4)
  implementation(libs.commons.io)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jackson.databind)
  implementation(libs.jakarta.validation.api)
  implementation(libs.slf4j.api)

  annotationProcessor(libs.lombok)
  compileOnly(libs.commons.dbcp2)
  compileOnly(libs.jackson.dataformat.yaml)
  compileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(libs.jackson.dataformat.yaml)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)

  testRuntimeOnly(libs.junit.jupiter.engine)

  testFixturesApi(project(":api"))
  testFixturesApi(libs.junit.jupiter.api)
  testFixturesImplementation(libs.commons.lang3)
}

val gitCommitIdPattern = Regex("[0-9a-fA-F]{40}")

// Ask git itself first: it handles linked worktrees, packed refs and detached HEAD.
// Only trust git when rootDir has its own .git, so a source tarball extracted inside another
// repository does not pick up that repository's commit id.
fun getGitCommitIdFromGit(): String? {
  if (!File(rootDir, ".git").exists()) {
    return null
  }
  return try {
    val process = ProcessBuilder("git", "rev-parse", "HEAD")
      .directory(rootDir)
      .redirectError(ProcessBuilder.Redirect.DISCARD)
      .start()
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly()
      return null
    }
    val output = process.inputStream.bufferedReader().use { it.readText().trim() }
    if (process.exitValue() == 0 && gitCommitIdPattern.matches(output)) output else null
  } catch (e: Exception) {
    null
  }
}

// Fallback that parses the .git directory by hand, used when the git command is unavailable.
fun getGitCommitIdFromFiles(): String {
  val gitPath = File(rootDir.path, ".git")
  val gitFolder = if (gitPath.isDirectory) {
    gitPath
  } else {
    // Linked worktrees have a ".git" file holding an absolute (or relative) "gitdir:" path.
    rootDir.resolve(gitPath.readText().trim().substringAfter("gitdir: ").trim())
  }

  val head = File(gitFolder, "HEAD").readText().split(":")
  if (head.size == 1) {
    return head[0].trim()
  }
  val refName = head[1].trim()
  // Branch refs of linked worktrees live in the common git dir.
  val commonDir = File(gitFolder, "commondir")
  val refFolders = if (commonDir.exists()) {
    listOf(gitFolder, gitFolder.resolve(commonDir.readText().trim()))
  } else {
    listOf(gitFolder)
  }
  for (folder in refFolders) {
    val refFile = File(folder, refName)
    if (refFile.exists()) {
      return refFile.readText().trim()
    }
    val packed = File(folder, "packed-refs")
    if (packed.exists()) {
      packed.readLines().firstOrNull { it.endsWith(" $refName") }?.let {
        return it.substringBefore(" ").trim()
      }
    }
  }
  throw GradleException("Unable to resolve ref $refName")
}

fun getGitCommitId(): String {
  getGitCommitIdFromGit()?.let { return it }
  return try {
    val commitId = getGitCommitIdFromFiles()
    if (!gitCommitIdPattern.matches(commitId)) {
      throw GradleException("Invalid Git commit id: $commitId")
    }
    commitId
  } catch (e: Exception) {
    println("WARN: Unable to get Git commit id : ${e.message}")
    ""
  }
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
    // The build info (commit id, compile date) is regenerated on every run, so never skip the jar.
    outputs.upToDateWhen { false }
    // The properties file is written in doFirst, after the cache key is computed, so a cached
    // jar from another commit with identical sources would carry a wrong commit id.
    outputs.cacheIf { false }

    doFirst() {
      writeProjectPropertiesFile()
      if (!file(propertiesFile).exists()) {
        throw GradleException("Failed to generate $propertiesFile.")
      }
      // processResources may have copied a stale file (e.g. empty commit id) into the main
      // resources output, which wins over the fresh one below; refresh it.
      val processed = layout.buildDirectory.file("resources/main/gravitino-build-info.properties")
        .get().asFile
      if (processed.parentFile.exists()) {
        file(propertiesFile).copyTo(processed, overwrite = true)
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
