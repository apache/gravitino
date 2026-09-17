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
import org.gradle.internal.os.OperatingSystem
import java.net.URI
import java.nio.file.Files
import java.security.MessageDigest
import java.util.Locale

val rumdlVersion = "0.2.60"

data class RumdlBinary(
  val triple: String,
  val sha256: String,
  val archiveExtension: String
)

fun resolveRumdlBinary(): RumdlBinary {
  val os = OperatingSystem.current()
  val arch = System.getProperty("os.arch").lowercase(Locale.US)
  val isArm = arch == "aarch64" || arch == "arm64"
  return when {
    os.isMacOsX && isArm ->
      RumdlBinary(
        "aarch64-apple-darwin",
        "f195e442ffa87fca71b3362333fd0c7b4818913ee84421917fa1a9638693b01f",
        "tar.gz"
      )
    os.isMacOsX ->
      RumdlBinary(
        "x86_64-apple-darwin",
        "bbd0618d6b2e3b4de94d85b228614b45462754a27e62c5560fb3c208e4aa719d",
        "tar.gz"
      )
    os.isLinux && isArm ->
      RumdlBinary(
        "aarch64-unknown-linux-gnu",
        "02a34cc98282f0f0799dcf9d3b5eac0cc84f98cda85df1c6486178dbc5451c54",
        "tar.gz"
      )
    os.isLinux ->
      RumdlBinary(
        "x86_64-unknown-linux-gnu",
        "84fc96856d21203b6482b7284aff5b539b7329a5cc078d7a94c55ae40d88752f",
        "tar.gz"
      )
    else ->
      throw GradleException(
        "markdownlint downloads rumdl for macOS and Linux only. Install rumdl $rumdlVersion " +
          "manually, or run the check on macOS/Linux."
      )
  }
}

fun sha256Hex(file: java.io.File): String {
  val digest = MessageDigest.getInstance("SHA-256")
  Files.newInputStream(file.toPath()).use { input ->
    val buffer = ByteArray(8192)
    var read = input.read(buffer)
    while (read != -1) {
      digest.update(buffer, 0, read)
      read = input.read(buffer)
    }
  }
  return digest.digest().joinToString("") { byte -> "%02x".format(byte) }
}

val rumdlHome = layout.buildDirectory.dir("rumdl/$rumdlVersion")
val rumdlBinary = rumdlHome.map { it.file("rumdl") }
val rumdlArtifact = resolveRumdlBinary()
val rumdlArchiveName = "rumdl-v$rumdlVersion-${rumdlArtifact.triple}.${rumdlArtifact.archiveExtension}"
val rumdlDownloadUrl =
  "https://github.com/rvben/rumdl/releases/download/v$rumdlVersion/$rumdlArchiveName"

val downloadRumdl by tasks.registering {
  group = "verification"
  description = "Download the pinned rumdl binary used by markdownlint."
  outputs.file(rumdlBinary)
  outputs.upToDateWhen { rumdlBinary.get().asFile.canExecute() }

  doLast {
    val homeDir = rumdlHome.get().asFile
    homeDir.mkdirs()
    val archive = homeDir.resolve(rumdlArchiveName)
    val binary = rumdlBinary.get().asFile
    if (!archive.isFile) {
      logger.lifecycle("Downloading rumdl $rumdlVersion from $rumdlDownloadUrl")
      URI.create(rumdlDownloadUrl).toURL().openStream().use { input ->
        Files.copy(input, archive.toPath())
      }
    }
    val actualSha = sha256Hex(archive)
    if (actualSha != rumdlArtifact.sha256) {
      archive.delete()
      throw GradleException(
        "Checksum mismatch for $rumdlArchiveName. Expected ${rumdlArtifact.sha256}, got $actualSha."
      )
    }
    exec {
      workingDir = homeDir
      commandLine("tar", "-xzf", archive.name)
    }
    if (!binary.isFile) {
      throw GradleException("rumdl binary was not found in $homeDir after extracting $rumdlArchiveName.")
    }
    binary.setExecutable(true, false)
  }
}

fun rumdlFailOn(): String = (findProperty("markdownlint.failOn") as String?) ?: "any"

fun rumdlCheckArgs(): List<String> {
  val args =
    mutableListOf(
      rumdlBinary.get().asFile.absolutePath,
      "check",
      "--fail-on",
      rumdlFailOn(),
      "design-docs"
    )
  if (System.getenv("GITHUB_ACTIONS") == "true") {
    args.addAll(listOf("--output-format", "github"))
  }
  return args
}

val markdownlint by tasks.registering(Exec::class) {
  group = "verification"
  description =
    "Lint design-docs/ with rumdl. Blocking locally; pass -Pmarkdownlint.failOn=never to warn only."
  dependsOn(downloadRumdl)
  workingDir = rootDir
  inputs.dir("design-docs")
  inputs.file(".rumdl.toml")
  commandLine("true")
  doFirst {
    commandLine(rumdlCheckArgs())
  }
}

val markdownlintFormat by tasks.registering(Exec::class) {
  group = "formatting"
  description = "Auto-format design-docs/ tables and other rumdl-fixable Markdown issues."
  dependsOn(downloadRumdl)
  workingDir = rootDir
  inputs.dir("design-docs")
  inputs.file(".rumdl.toml")
  commandLine("true")
  doFirst {
    commandLine(rumdlBinary.get().asFile.absolutePath, "fmt", "design-docs")
  }
}

val markdownlintSelfCheck by tasks.registering(Exec::class) {
  group = "verification"
  description =
    "Run fixture tests for the Markdown lint Gradle/CI wiring (blocking in CI)."
  dependsOn(downloadRumdl)
  workingDir = rootDir
  inputs.file(".rumdl.toml")
  inputs.file(".github/workflows/design-docs-markdownlint.yml")
  inputs.file("dev/ci/test_markdownlint.sh")
  inputs.dir("dev/ci/markdownlint-fixtures")
  commandLine("true")
  doFirst {
    commandLine(
      "bash",
      "dev/ci/test_markdownlint.sh",
      rumdlBinary.get().asFile.absolutePath
    )
  }
}
