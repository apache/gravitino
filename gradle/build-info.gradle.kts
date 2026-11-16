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

// Generates gravitino-build-info.properties (project version, compile date, git commit id) and
// wires it into processResources. Applied from common/build.gradle.kts; requires the java plugin.

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

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

// The build info file is generated into the build directory by a dedicated task. Its inputs are
// the project version and the git commit id, so it (and everything downstream, including the
// jar and the build cache) is only redone when the commit changes.
val gitCommitIdValue: String by lazy { getGitCommitId() }
val buildInfoFileName = "gravitino-build-info.properties"
val buildInfoDir = layout.buildDirectory.dir("generated/build-info")

val generateBuildInfo by tasks.registering {
  val projectVersion = project.version.toString()
  val outputFile = buildInfoDir.map { it.file(buildInfoFileName) }

  inputs.property("projectVersion", projectVersion)
  inputs.property("gitCommitId", providers.provider { gitCommitIdValue })
  outputs.file(outputFile)

  doLast {
    val dateFormat = SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val file = outputFile.get().asFile
    file.parentFile.mkdirs()
    file.writer().use { writer ->
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
      writer.write("compile.date=${dateFormat.format(Date())}\n")
      writer.write("git.commit.id=$gitCommitIdValue\n")
    }
  }
}

the<SourceSetContainer>().named("main") {
  // Ignore a stale copy generated into src/main/resources by older builds.
  resources.exclude(buildInfoFileName)
}

tasks.named<ProcessResources>("processResources") {
  from(generateBuildInfo)
}

tasks.named<Delete>("clean") {
  // Remove the stale copy generated into src/main/resources by older builds.
  delete("src/main/resources/$buildInfoFileName")
}
