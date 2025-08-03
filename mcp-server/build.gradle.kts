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

import org.gradle.api.GradleException
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import org.gradle.kotlin.dsl.register
import java.io.File

val pythonProjectDir = project.projectDir
val venvDir = pythonProjectDir.resolve(".venv")

val systemPython = when {
  System.getProperty("os.name").contains("win", ignoreCase = true) -> "python"
  else -> "python3"
}

val globalUvExecutable = when {
  System.getProperty("os.name").contains("win", ignoreCase = true) -> "uv.exe"
  else -> "uv"
}

val venvPython = when {
  System.getProperty("os.name").contains("win", ignoreCase = true) ->
    venvDir.resolve("Scripts/python.exe").absolutePath
  else ->
    venvDir.resolve("bin/python").absolutePath
}

tasks {
  register<Exec>("checkSystemPython") {
    group = "python"
    description = "Check system Python availability"
    commandLine(systemPython, "--version")

    doLast {
      if (executionResult.get().exitValue != 0) {
        throw GradleException("System Python not found. Please install Python and ensure '$systemPython' is in PATH.")
      }
    }
  }

  register<Exec>("installGlobalUv") {
    group = "python"
    description = "Install UV globally if not present"
    dependsOn("checkSystemPython")

    onlyIf {
      try {
        exec {
          commandLine(globalUvExecutable, "--version")
          isIgnoreExitValue = true
        }.exitValue != 0
      } catch (e: Exception) {
        true
      }
    }

    doFirst {
      logger.lifecycle("UV not found, installing via official script...")
    }

    val isWindows = System.getProperty("os.name").contains("win", ignoreCase = true)
    if (isWindows) {
      commandLine("powershell", "-Command", "irm https://astral.sh/uv/install.ps1 | iex")
    } else {
      commandLine("/bin/sh", "-c", "curl -LsSf https://astral.sh/uv/install.sh | sh")
    }

    doLast {
      val uvCheck = exec {
        commandLine(globalUvExecutable, "--version")
        isIgnoreExitValue = true
      }
      if (uvCheck.exitValue != 0) {
        throw GradleException("UV installation failed. Please check the installation script.")
      }
    }
  }

  register<Exec>("createVenvWithUv") {
    group = "python"
    description = "Create Python virtual environment using global UV"
    dependsOn("installGlobalUv")
    workingDir(pythonProjectDir)

    commandLine(globalUvExecutable, "venv", venvDir.absolutePath)

    doLast {
      if (executionResult.get().exitValue != 0) {
        throw GradleException("Failed to create virtual environment with UV. Exit code: ${executionResult.get().exitValue}")
      }

      if (!File(venvPython).exists()) {
        throw GradleException("Virtual environment creation failed. Python executable not found at: $venvPython")
      }
    }
  }

  register<Exec>("installDependenciesWithUv") {
    group = "python"
    description = "Install Python dependencies using global UV"
    dependsOn("createVenvWithUv")
    workingDir(pythonProjectDir)

    commandLine(globalUvExecutable, "pip", "install", "--python", venvPython, "-e", ".")

    doLast {
      if (executionResult.get().exitValue != 0) {
        throw GradleException("Failed to install dependencies. Exit code: ${executionResult.get().exitValue}")
      }
    }
  }

  register<Exec>("installFormatTools") {
    group = "python"
    description = "Install code formatting tools"
    dependsOn("createVenvWithUv")
    workingDir(pythonProjectDir)

    commandLine(globalUvExecutable, "pip", "install", "--python", venvPython, "black", "isort")

    doLast {
      if (executionResult.get().exitValue != 0) {
        throw GradleException("Failed to install formatting tools. Exit code: ${executionResult.get().exitValue}")
      }
    }
  }

  register("buildPython") {
    group = "python"
    description = "Build Python project"
    dependsOn("installDependenciesWithUv")
    doLast {
      logger.lifecycle("Python project built successfully")
    }
  }

  register<Exec>("testPython") {
    group = "python"
    description = "Run Python unit tests with unittest"
    dependsOn("buildPython")
    workingDir(pythonProjectDir)

    commandLine(venvPython, "-m", "unittest", "discover", "-s", "tests", "-v")

    doLast {
      if (executionResult.get().exitValue != 0) {
        throw GradleException("Unit tests failed. Exit code: ${executionResult.get().exitValue}")
      }
    }
  }

  register<Delete>("cleanPython") {
    group = "python"
    description = "Clean Python build artifacts"
    delete(venvDir)
    delete(pythonProjectDir.resolve("dist"))
    delete(pythonProjectDir.resolve("build"))
    delete(pythonProjectDir.resolve("*.egg-info"))
  }

  // 9. Apply Python code formatting (fixed)
  register<DefaultTask>("formatApplyPython") {
    group = "python"
    description = "Apply Black formatting and isort import sorting"
    dependsOn("installFormatTools")

    doLast {
      // Apply isort
      exec {
        workingDir = pythonProjectDir
        commandLine(venvPython, "-m", "isort", "mcp_server", "tests")
      }

      // Apply Black
      exec {
        workingDir = pythonProjectDir
        commandLine(venvPython, "-m", "black", "mcp_server", "tests")
      }

      logger.lifecycle("Python formatting applied (isort + Black)")
    }
  }

  // 10. Check Python code formatting (fixed to run both checks)
  register<DefaultTask>("formatCheckPython") {
    group = "python"
    description = "Check Python code formatting with Black and import order with isort"
    dependsOn("installFormatTools")

    doLast {
      var anyFailed = false

      // 执行 isort 检查
      val isortExitCode = exec {
        workingDir = pythonProjectDir
        commandLine(venvPython, "-m", "isort", "--check", "mcp_server", "tests")
        isIgnoreExitValue = false
      }.exitValue

      if (isortExitCode != 0) {
        throw GradleException("Python isort formatting check failed")
      }

      // 执行 Black 检查
      val blackExitCode = exec {
        workingDir = pythonProjectDir
        commandLine(venvPython, "-m", "black", "--check", "mcp_server", "tests")
        isIgnoreExitValue = false
      }.exitValue

      if (blackExitCode != 0) {
        throw GradleException("Python black formatting check failed")
        anyFailed = true
      }
    }
  }

  register<Exec>("generateLockfile") {
    group = "python"
    description = "Generate lock file using global UV"
    dependsOn("createVenvWithUv")
    workingDir(pythonProjectDir)
    commandLine(globalUvExecutable, "pip", "compile", "pyproject.toml", "--python", venvPython, "-o", "requirements.lock")
  }
}

tasks.named("test") {
  dependsOn("testPython")
}

tasks.named("build") {
  dependsOn("buildPython")
}

tasks.named("clean") {
  dependsOn("cleanPython")
}

tasks.named("spotlessCheck") {
  dependsOn("formatCheckPython")
}

tasks.named("spotlessApply") {
  dependsOn("formatApplyPython")
}
