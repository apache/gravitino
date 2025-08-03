import org.gradle.api.GradleException
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import org.gradle.kotlin.dsl.register
import java.io.File

val pythonProjectDir = project.projectDir
val venvDir = pythonProjectDir.resolve(".venv")

// 确定系统Python命令
val systemPython = when {
  System.getProperty("os.name").contains("win", ignoreCase = true) -> "python"
  else -> "python3"
}

// 定义全局UV可执行文件路径
val globalUvExecutable = when {
  System.getProperty("os.name").contains("win", ignoreCase = true) -> "uv.exe"
  else -> "uv"
}

// 定义虚拟环境中的可执行文件路径
val venvPython = when {
  System.getProperty("os.name").contains("win", ignoreCase = true) ->
    venvDir.resolve("Scripts/python.exe").absolutePath
  else ->
    venvDir.resolve("bin/python").absolutePath
}

tasks {
  // 1. 检查系统Python是否可用
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

  // 2. 安装全局UV（如果尚未安装）
  register<Exec>("installGlobalUv") {
    group = "python"
    description = "Install UV globally if not present"
    dependsOn("checkSystemPython")

    // 先检查UV是否已安装
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
      // 验证UV是否安装成功
      val uvCheck = exec {
        commandLine(globalUvExecutable, "--version")
        isIgnoreExitValue = true
      }
      if (uvCheck.exitValue != 0) {
        throw GradleException("UV installation failed. Please check the installation script.")
      }
    }
  }

  // 3. 使用全局UV创建虚拟环境
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

      // 验证虚拟环境是否创建成功
      if (!File(venvPython).exists()) {
        throw GradleException("Virtual environment creation failed. Python executable not found at: $venvPython")
      }
    }
  }

  // 4. 使用全局UV安装项目依赖
  register<Exec>("installDependenciesWithUv") {
    group = "python"
    description = "Install Python dependencies using global UV"
    dependsOn("createVenvWithUv")
    workingDir(pythonProjectDir)

    // 使用全局UV安装依赖到虚拟环境
    commandLine(globalUvExecutable, "pip", "install", "--python", venvPython, "-e", ".")

    doLast {
      if (executionResult.get().exitValue != 0) {
        throw GradleException("Failed to install dependencies. Exit code: ${executionResult.get().exitValue}")
      }
    }
  }

  // 5. 安装格式化工具（Black）
  register<Exec>("installFormatTools") {
    group = "python"
    description = "Install code formatting tools"
    dependsOn("createVenvWithUv")
    workingDir(pythonProjectDir)

    // 使用全局UV安装Black到虚拟环境
    commandLine(globalUvExecutable, "pip", "install", "--python", venvPython, "black")

    doLast {
      if (executionResult.get().exitValue != 0) {
        throw GradleException("Failed to install formatting tools. Exit code: ${executionResult.get().exitValue}")
      }
    }
  }

  // 6. Python 构建任务
  register("buildPython") {
    group = "python"
    description = "Build Python project"
    dependsOn("installDependenciesWithUv")
    doLast {
      logger.lifecycle("Python project built successfully")
    }
  }

  // 7. 运行单元测试（使用Python内置unittest）
  register<Exec>("testPython") {
    group = "python"
    description = "Run Python unit tests with unittest"
    dependsOn("buildPython")
    workingDir(pythonProjectDir)

    // 使用unittest发现并运行测试
    commandLine(venvPython, "-m", "unittest", "discover", "-s", "tests", "-v")

    doLast {
      if (executionResult.get().exitValue != 0) {
        throw GradleException("Unit tests failed. Exit code: ${executionResult.get().exitValue}")
      }
    }
  }

  // 8. 清理任务
  register<Delete>("cleanPython") {
    group = "python"
    description = "Clean Python build artifacts"
    delete(venvDir)
    delete(pythonProjectDir.resolve("dist"))
    delete(pythonProjectDir.resolve("build"))
    delete(pythonProjectDir.resolve("*.egg-info"))
  }

  // 9. 应用代码格式化（使用虚拟环境的Python）
  register<Exec>("formatApplyPython") {
    group = "python"
    description = "Apply Black formatting to Python code"
    dependsOn("installFormatTools") // 确保Black已安装
    workingDir(pythonProjectDir)
    commandLine(venvPython, "-m", "black", "mcp_server", "tests")

    doLast {
      logger.lifecycle("Black formatting applied successfully")
    }
  }

  // 10. 检查代码格式（使用虚拟环境的Python）
  register<Exec>("formatCheckPython") {
    group = "python"
    description = "Check Python code formatting with Black"
    dependsOn("installFormatTools") // 确保Black已安装
    workingDir(pythonProjectDir)
    commandLine(venvPython, "-m", "black", "--check", "--diff", "mcp_server", "tests")

    isIgnoreExitValue = true

    doLast {
      when (val exitCode = executionResult.get().exitValue) {
        0 -> logger.lifecycle("Black check passed. Code is formatted correctly.")
        1 -> throw GradleException("Black found formatting issues! Run 'formatApplyPython' to fix them.")
        else -> throw GradleException("Black check failed with exit code: $exitCode")
      }
    }
  }

  // 11. 生成锁文件（使用全局UV）
  register<Exec>("generateLockfile") {
    group = "python"
    description = "Generate lock file using global UV"
    dependsOn("createVenvWithUv")
    workingDir(pythonProjectDir)
    commandLine(globalUvExecutable, "pip", "compile", "pyproject.toml", "--python", venvPython, "-o", "requirements.lock")
  }
}

// 集成 Python 任务到主构建生命周期
tasks.named("check") {
  dependsOn("testPython")
  dependsOn("formatCheckPython")
}

// 注意：已移除 packagePython 任务及其在构建中的依赖
tasks.named("build") {
  dependsOn("buildPython")
}

tasks.named("clean") {
  dependsOn("cleanPython")
}
