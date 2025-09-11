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
import io.github.piyushroshan.python.VenvTask
import java.net.HttpURLConnection
import java.net.URL

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

  println("Waiting for server to be ready at $urlString (timeout: ${timeout / 1000}s)...")
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
          val duration = System.currentTimeMillis() - startTime
          println("\nServer is ready! Took ${duration / 1000.0} seconds.")
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

    // Print a dot to indicate waiting
    print(".")
    System.out.flush() // Ensure the dot is printed immediately
    Thread.sleep(500)  // Wait for 0.5 second before checking again
  }
}

fun gravitinoServer(operation: String) {
    val process = ProcessBuilder("${project.rootDir.path}/distribution/package/bin/gravitino.sh", operation).start()
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
    val docsUrl = "https://gravitino.apache.org/docs/latest"

    // Use regular expression to match the `[](./a/b/c.md?language=python)` or `[](./a/b/c.md#arg1)` link in the content
    // Convert `[](./a/b/c.md?language=python)` to `[](https://gravitino.apache.org/docs/latest/a/b/c/language=python)`
    val patternDocs = Regex("""(?<!!)\[([^\]]+)]\(\.\/([^)]+)\.md([?#][^)]+)?\)""")
    val contentUpdateDocs = patternDocs.replace(content) { matchResult ->
      val text = matchResult.groupValues[1]
      val path = matchResult.groupValues[2]
      val params = matchResult.groupValues[3]
      "[$text]($docsUrl/$path/$params)"
    }

    // Use regular expression to match the `![](./a/b/c.png)` link in the content
    // Convert `![](./a/b/c.png)` to `[](https://github.com/apache/gravitino/blob/main/docs/a/b/c.png?raw=true)`
    val assertUrl = "https://github.com/apache/gravitino/blob/main/docs"
    val patternImage = """!\[([^\]]+)]\(\./assets/([^)]+)\)""".toRegex()
    val contentUpdateImage = patternImage.replace(contentUpdateDocs) { matchResult ->
      val altText = matchResult.groupValues[1]
      val fileName = matchResult.groupValues[2]
      "![${altText}]($assertUrl/assets/$fileName?raw=true)"
    }

    val readmeFile = file("README.md")
    readmeFile.writeText(contentUpdateImage)
  } catch (e: Exception) {
    throw GradleException("Failed to generate client-python/README.md.")
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

  val build by registering(VenvTask::class) {
    dependsOn(pylint)
    venvExec = "python"
    args = listOf("scripts/generate_version.py")
  }

  val integrationTest by registering(VenvTask::class) {
    doFirst {
      gravitinoServer("start")
    }

    venvExec = "coverage"
    args = listOf("run", "--branch", "-m", "unittest")
    workingDir = projectDir.resolve("./tests/integration")
    val dockerTest = project.rootProject.extra["dockerTest"] as? Boolean ?: false
    val envMap = mapOf<String, Any>().toMutableMap()
    if (dockerTest) {
      envMap.putAll(mapOf(
        "PYTHON_BUILD_PATH" to project.rootDir.path + "/clients/client-python/build"
      ))
    }
    envMap.putAll(mapOf(
      "PROJECT_VERSION" to project.version,
      "GRAVITINO_HOME" to project.rootDir.path + "/distribution/package",
      "START_EXTERNAL_GRAVITINO" to "true",
      "DOCKER_TEST" to dockerTest.toString(),
      "GRAVITINO_CI_HIVE_DOCKER_IMAGE" to "apache/gravitino-ci:hive-0.1.13",
      "GRAVITINO_OAUTH2_SAMPLE_SERVER" to "datastrato/sample-authorization-server:0.3.0",
      // Set the PYTHONPATH to the client-python directory, make sure the tests can import the
      // modules from the client-python directory.
      "PYTHONPATH" to "${project.rootDir.path}/clients/client-python"
    ))
    environment = envMap

    doLast {
      gravitinoServer("stop")
    }
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

    environment = mapOf(
      // Set the PYTHONPATH to the client-python directory, make sure the tests can import the
      // modules from the client-python directory.
      "PYTHONPATH" to "${project.rootDir.path}/clients/client-python"
    )

    finalizedBy(unitCoverageReport)
  }

  val test by registering(VenvTask::class) {
    val skipUTs = project.hasProperty("skipTests")
    val skipITs = project.hasProperty("skipITs")
    val skipAllTests = skipUTs && skipITs
    if (!skipAllTests) {
      dependsOn(pipInstall, pylint)
      if (!skipUTs) {
        dependsOn(unitTests)
      }
      if (!skipITs) {
        dependsOn(integrationTest)
      }
    }
  }

  val doc by registering(VenvTask::class) {
    workingDir = projectDir.resolve("./docs")
    venvExec = "make"
    args = listOf("html")
  }

  val distribution by registering(VenvTask::class) {
    dependsOn(build)
    doFirst {
      delete("README.md")
      generatePypiProjectHomePage()
      delete("dist")
      copy {
        into("${project.rootDir}/clients/client-python")
        rename { fileName ->
          fileName.replace(".bin", "")
        }
      }
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
    delete("docs/build")
    delete("docs/source/generated")
    delete("gravitino/version.ini")
    delete("apache_gravitino.egg-info")
    delete("tests/unittests/htmlcov")
    delete("tests/unittests/.coverage")
    delete("tests/integration/htmlcov")
    delete("tests/integration/.coverage")

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
