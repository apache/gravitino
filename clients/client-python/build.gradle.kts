/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
import io.github.piyushroshan.python.VenvTask
import java.net.HttpURLConnection
import org.gradle.internal.os.OperatingSystem
import java.io.BufferedWriter
import java.io.FileWriter
import java.net.URL
import java.nio.file.Files
import java.nio.file.Paths

plugins {
  id("io.github.piyushroshan.python-gradle-miniforge-plugin") version "1.0.0"
}

pythonPlugin {
  pythonVersion.set(project.rootProject.extra["pythonVersion"].toString())
}

fun deleteCacheDir(targetDir: String) {
  project.fileTree(project.projectDir).matching {
    include("**/$targetDir/**")
  }.forEach { file ->
    val targetDirPath = file.path.substring(0, file.path.lastIndexOf(targetDir) + targetDir.length)
    project.file(targetDirPath).deleteRecursively()
  }
}


fun waitForServerIsReady(host: String = "http://localhost", port: Int = 8090, timeout: Long = 30000) {
  val startTime = System.currentTimeMillis()
  var exception: java.lang.Exception?
  val urlString = "$host:$port/metrics"
  val successPattern = Regex("\"version\"\\s*:")

  while (true) {
    try {
      val url = URL(urlString)
      val connection = url.openConnection() as HttpURLConnection
      connection.requestMethod = "GET"
      connection.connectTimeout = 1000
      connection.readTimeout = 1000

      val responseCode = connection.responseCode
      if (responseCode == 200) {
        val response = connection.inputStream.bufferedReader().use { it.readText() }
        if (successPattern.containsMatchIn(response)) {
          return  // If this succeeds, the API is up and running
        } else {
          exception = RuntimeException("API returned unexpected response: $response")
        }
      } else {
        exception = RuntimeException("Received non-200 response code: $responseCode")
      }
    } catch (e: Exception) {
      // API is not available yet, continue to wait
      exception = e
    }

    if (System.currentTimeMillis() - startTime > timeout) {
      throw RuntimeException("Timed out waiting for API to be available", exception)
    }
    Thread.sleep(500)  // Wait for 0.5 second before checking again
  }
}

fun gravitinoServer(operation: String) {
  val processBuilder = ProcessBuilder("${project.rootDir.path}/distribution/package/bin/gravitino.sh", operation)
  processBuilder.environment()["HADOOP_USER_NAME"] = "datastrato"
  val process = processBuilder.start()
  val exitCode = process.waitFor()
  if (exitCode == 0) {
    val currentContext = process.inputStream.bufferedReader().readText()
    if (operation == "start") {
      waitForServerIsReady()
    }
    println("Gravitino server status: $currentContext")
  } else {
    println("Gravitino server execution failed with exit code $exitCode")
  }
}

fun startHiveContainer() {
  val launchScript = "${project.rootDir.path}/clients/client-python/tests/integration/init/launch.sh"
  val processBuilder = ProcessBuilder("bash", "-c", launchScript)
  val logPath = System.getenv("gravitino.log.path")
  if (logPath != null && logPath.isNotBlank()) {
    val env = mapOf("GRAVITINO_LOG_PATH" to logPath)
    processBuilder.environment().putAll(env)
  }
  val process = processBuilder.start()
  val exitCode = process.waitFor()
  if (exitCode != 0) {
    val output = process.inputStream.bufferedReader().readText()
    throw RuntimeException("Hive container started failed with exit code $exitCode, msg: $output")
  }
}

fun resolveDockerAddress(): Map<String, String> {
  val resolveAddressScript = "${project.rootDir.path}/clients/client-python/tests/integration/init/inspect_ip.sh"
  val process = ProcessBuilder("bash", "-c", resolveAddressScript).start()

  val exitCode = process.waitFor()
  val output = process.inputStream.bufferedReader().readText()
  if (exitCode == 0) {
    val lines = output.lines()
    // expect the output to be like:
    // hive:10.20.30.19
    val addressMap = mutableMapOf<String, String>()
    lines.stream()
       .filter { line -> line.isNotBlank() }
       .forEach { line ->
          val (name, ip) = line.split(":").let { (name, ip) -> Pair(name, ip) }
          if (name == "hive") {
            if (ip.isBlank()) {
              throw RuntimeException("Hive container address is blank.")
            }
            addressMap["hive"] = ip
          }
      }
    return addressMap
  }
  throw RuntimeException("Docker container address resolved failed with exit code $exitCode, msg: $output")
}

fun appendServerHadoopConf(hiveContainerAddress: String) {
  val hadoopConfPath = "${project.rootDir.path}/distribution/package/catalogs/hadoop/conf/hadoop.conf"
  val confFile = File(hadoopConfPath)
  if (!confFile.exists()) {
    throw RuntimeException("Hadoop conf file is not found at `$hadoopConfPath`.")
  }
  val confs = mapOf(
    "gravitino.bypass.fs.defaultFS" to "hdfs://$hiveContainerAddress:9000"
  )

  BufferedWriter(FileWriter(confFile, true)).use { writer ->
    confs.forEach { (key, value) ->
      writer.append("\n$key = $value")
    }
  }
}

val hadoopVersion = "2.7.3"
val hadoopPackName = "hadoop-${hadoopVersion}.tar.gz"
val hadoopDownloadUrl = "https://archive.apache.org/dist/hadoop/core/hadoop-${hadoopVersion}/${hadoopPackName}"
val localArchiveDir = "${project.rootDir.path}/clients/client-python/it-archive"
fun getAndUnzipHadoopPack() {
  if (!File(localArchiveDir).exists()) {
    throw RuntimeException("Local archive directory is not found at `$localArchiveDir`.")
  }
  val targetFile = File(localArchiveDir, hadoopPackName)
  if (!targetFile.exists()) {
    // Download the Hadoop distribution pack
    targetFile.outputStream().use { output ->
      URL(hadoopDownloadUrl).openStream().use { input ->
        input.copyTo(output)
      }
    }
  }
  if (!targetFile.exists()) {
    throw RuntimeException("Target Hadoop distribution pack does not exist: ${targetFile.absolutePath}.")
  }
  // Unzip the Hadoop distribution pack
  val unzipProcess = ProcessBuilder(
          "tar", "-xvf", "${localArchiveDir}/${hadoopPackName}", "-C", localArchiveDir)
          .redirectOutput(ProcessBuilder.Redirect.INHERIT)
          .redirectError(ProcessBuilder.Redirect.INHERIT)
          .start()
  val exitCode = unzipProcess.waitFor()
  if (exitCode != 0) {
    val output = unzipProcess.inputStream.bufferedReader().readText()
    throw RuntimeException("Unzip Hadoop distribution pack failed with exit code $exitCode, msg: $output")
  }

  // Replace the Hadoop native libs for macOS
  if (OperatingSystem.current().isMacOsX()) {
    val hadoopNativeLibsDir = File("${localArchiveDir}/hadoop-${hadoopVersion}/lib/native")
    if (hadoopNativeLibsDir.exists()) {
      // Backup the original native libs
      val hadoopNativeLibsDirBack = File("${hadoopNativeLibsDir.absolutePath}_bak")
      hadoopNativeLibsDirBack.deleteRecursively()
      Files.move(hadoopNativeLibsDir.toPath(), hadoopNativeLibsDirBack.toPath())
    }
    val cpuArch = System.getProperty("os.arch")
    val macosNativeLibPath = if (cpuArch.contains("arm") || cpuArch.contains("aarch64")) {
      Paths.get("${project.rootDir.path}/clients/client-python/tests/integration/hadoop-${hadoopVersion}/macos/arm/native")
    } else {
      Paths.get("${project.rootDir.path}/clients/client-python/tests/integration/hadoop-${hadoopVersion}/macos/intel/native")
    }
    Files.walk(macosNativeLibPath).forEach { source ->
      val target = hadoopNativeLibsDir.toPath().resolve(macosNativeLibPath.relativize(source))
      Files.copy(source, target)
    }
  }
}

fun getHadoopClasspathEnv(): String {
  val hdfsShellPath = "${localArchiveDir}/hadoop-${hadoopVersion}/bin/hadoop"
  val process = ProcessBuilder(hdfsShellPath, "classpath", "--glob").start()
  val exitCode = process.waitFor()
  val output = process.inputStream.bufferedReader().readText()
  if (exitCode == 0) {
    return output
  } else {
    throw RuntimeException("Hive container started failed with exit code $exitCode, msg: $output")
  }
}

fun stopHiveContainer() {
  val shutdownScript = "${project.rootDir.path}/clients/client-python/tests/integration/init/shutdown.sh"
  val process = ProcessBuilder("bash", "-c", shutdownScript).start()
  val exitCode = process.waitFor()
  if (exitCode != 0) {
    val output = process.inputStream.bufferedReader().readText()
    throw RuntimeException("Hive container stopped failed with exit code $exitCode, msg: $output")
  }
}

fun generatePypiProjectHomePage() {
  try {
    val inputFile = file("${project.rootDir}/docs/how-to-use-python-client.md")
    val outputFile = file("README.md")

    // Copy the contents of the how-to-use-python-client.md file to the README.md file for PyPi
    // release, remove the front matter as PyPi doesn't support README file contains front
    // matter.
    val lines = inputFile.readLines()
    var skipFrontMatterHead = false
    var skipFrontMatterEnd = false
    for (line in lines) {
      if (line.trim() == "---") {
        if (!skipFrontMatterHead) {
          skipFrontMatterHead = true
          continue
        } else if (!skipFrontMatterEnd) {
          skipFrontMatterEnd = true
          continue
        }
      }
      if (skipFrontMatterHead && skipFrontMatterEnd) {
        outputFile.appendText(line + "\n")
      }
    }

    // Because the README.md file is generated from the how-to-use-python-client.md file, the
    // relative path of the images in the how-to-use-python-client.md file is incorrect. We need
    // to fix the relative path of the images/markdown to the absolute path.
    val content = outputFile.readText()
    val docsUrl = "https://datastrato.ai/docs/latest"

    // Use regular expression to match the `[](./a/b/c.md?language=python)` or `[](./a/b/c.md#arg1)` link in the content
    // Convert `[](./a/b/c.md?language=python)` to `[](https://datastrato.ai/docs/latest/a/b/c/language=python)`
    val patternDocs = Regex("""(?<!!)\[([^\]]+)]\(\.\/([^)]+)\.md([?#][^)]+)?\)""")
    val contentUpdateDocs = patternDocs.replace(content) { matchResult ->
      val text = matchResult.groupValues[1]
      val path = matchResult.groupValues[2]
      val params = matchResult.groupValues[3]
      "[$text]($docsUrl/$path/$params)"
    }

    // Use regular expression to match the `![](./a/b/c.png)` link in the content
    // Convert `![](./a/b/c.png)` to `[](https://raw.githubusercontent.com/datastrato/gravitino/main/docs/a/b/c.png)`
    val assertUrl = "https://raw.githubusercontent.com/datastrato/gravitino/main/docs"
    val patternImage = """!\[([^\]]+)]\(\./assets/([^)]+)\)""".toRegex()
    val contentUpdateImage = patternImage.replace(contentUpdateDocs) { matchResult ->
      val altText = matchResult.groupValues[1]
      val fileName = matchResult.groupValues[2]
      "![${altText}]($assertUrl/assets/$fileName)"
    }

    val readmeFile = file("README.md")
    readmeFile.writeText(contentUpdateImage)
  } catch (e: Exception) {
    throw GradleException("client-python README.md file not generated!")
  }
}

tasks {
  val pipInstall by registering(VenvTask::class) {
    venvExec = "pip"
    args = listOf("install", "-e", ".[dev]")
  }

  val black by registering(VenvTask::class) {
    dependsOn(pipInstall)
    venvExec = "black"
    args = listOf("./gravitino", "./tests", "./scripts")
  }

  val pylint by registering(VenvTask::class) {
    dependsOn(pipInstall)
    mustRunAfter(black)
    venvExec = "pylint"
    args = listOf("./gravitino", "./tests", "./scripts")
  }

  val integrationCoverageReport by registering(VenvTask::class){
    venvExec = "coverage"
    args = listOf("html")
    workingDir = projectDir.resolve("./tests/integration")
  }

  val integrationTest by registering(VenvTask::class) {
    doFirst {
      startHiveContainer()
      val addressMap = resolveDockerAddress()
      val hiveContainerAddress = addressMap["hive"] ?: throw RuntimeException("Hive container address is null.")
      appendServerHadoopConf(hiveContainerAddress)
      getAndUnzipHadoopPack()
      val hdfsClasspath = getHadoopClasspathEnv()
      environment = mapOf(
          "PROJECT_VERSION" to project.version,
          "GRAVITINO_HOME" to project.rootDir.path + "/distribution/package",
          "START_EXTERNAL_GRAVITINO" to "true",
          "GRAVITINO_PYTHON_HIVE_ADDRESS" to hiveContainerAddress,
          "HADOOP_USER_NAME" to "datastrato",
          "HADOOP_HOME" to "${localArchiveDir}/hadoop-${hadoopVersion}",
          "HADOOP_CONF_DIR" to "${localArchiveDir}/hadoop-${hadoopVersion}/etc/hadoop",
          "CLASSPATH" to hdfsClasspath
      )
      gravitinoServer("start")
    }

    venvExec = "coverage"
    args = listOf("run", "--branch", "-m", "unittest")
    workingDir = projectDir.resolve("./tests/integration")


    doLast {
      gravitinoServer("stop")
      stopHiveContainer()
    }

    finalizedBy(integrationCoverageReport)
  }

  val unitCoverageReport by registering(VenvTask::class){
    venvExec = "coverage"
    args = listOf("html")
    workingDir = projectDir.resolve("./tests/unittests")
  }

  val unitTests by registering(VenvTask::class) {
    venvExec = "coverage"
    args = listOf("run", "--branch", "-m", "unittest")
    workingDir = projectDir.resolve("./tests/unittests")

    finalizedBy(unitCoverageReport)
  }

  val test by registering(VenvTask::class) {
    dependsOn(pipInstall, pylint, unitTests)

    val skipPyClientITs = project.hasProperty("skipPyClientITs")
    val skipITs = project.hasProperty("skipITs")
    if (!skipITs && !skipPyClientITs) {
      dependsOn(integrationTest)
    }
  }

  val build by registering(VenvTask::class) {
    dependsOn(pylint)
    venvExec = "python"
    args = listOf("scripts/generate_version.py")
  }

  val pydoc by registering(VenvTask::class) {
    venvExec = "python"
    args = listOf("scripts/generate_doc.py")
  }

  val distribution by registering(VenvTask::class) {
    dependsOn(build)
    doFirst {
      delete("README.md")
      generatePypiProjectHomePage()
      delete("dist")
    }

    venvExec = "python"
    args = listOf("setup.py", "sdist")

    doLast {
      delete("README.md")
    }
  }

  // Deploy to https://pypi.org/project/gravitino/
  val deploy by registering(VenvTask::class) {
    dependsOn(distribution)
    val twine_password = System.getenv("TWINE_PASSWORD")
    venvExec = "twine"
    args = listOf("upload", "dist/*", "-p${twine_password}")
  }

  val clean by registering(Delete::class) {
    delete("build")
    delete("dist")
    delete("docs")
    delete("version.ini")
    delete("gravitino.egg-info")
    delete("tests/unittests/htmlcov")
    delete("tests/unittests/.coverage")
    delete("tests/integration/htmlcov")
    delete("tests/integration/.coverage")
    delete("it-archive")

    doLast {
      deleteCacheDir(".pytest_cache")
      deleteCacheDir("__pycache__")
    }
  }

  matching {
    it.name.endsWith("envSetup")
  }.all {
    // add install package and code formatting before any tasks
    finalizedBy(pipInstall, black, pylint, build)
  }
}
