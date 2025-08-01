import java.nio.file.Paths
import org.gradle.internal.os.OperatingSystem

plugins {
    base
}

// 项目配置
group = "com.example"
version = "1.0.0"

// 环境配置
val projectDir = project.layout.projectDirectory.asFile
val buildDir = layout.buildDirectory.get().asFile
val venvDir = file("$buildDir/venv")
val pythonExecutable = if (OperatingSystem.current().isWindows) {
    Paths.get(venvDir.path, "Scripts", "python.exe").toString()
} else {
    Paths.get(venvDir.path, "bin", "python").toString()
}
val uvExecutable = if (OperatingSystem.current().isWindows) {
    Paths.get(venvDir.path, "Scripts", "uv.exe").toString()
} else {
    Paths.get(venvDir.path, "bin", "uv").toString()
}

// 基础环境检查
tasks.register("checkPython") {
    group = "Setup"
    description = "Check Python installation"

    doLast {
        val pythonVersion = "3.10"
        val checkProcess = ProcessBuilder("python", "--version").start()
        checkProcess.waitFor()

        if (checkProcess.exitValue() != 0) {
            throw GradleException("Python is not installed. Please install Python $pythonVersion")
        }

        val output = checkProcess.inputStream.bufferedReader().readText()
        if (!output.contains(pythonVersion)) {
            throw GradleException("Required Python $pythonVersion not found. Found: $output")
        }
    }
}

// 安装 UV
tasks.register("installUv") {
    group = "Setup"
    description = "Install UV package manager"

    dependsOn("checkPython")

    doLast {
        // 创建虚拟环境
        exec {
            commandLine("python", "-m", "venv", venvDir)
        }

        // 安装 UV
        exec {
            commandLine(
                if (OperatingSystem.current().isWindows)
                    "$venvDir/Scripts/python" else "$venvDir/bin/python",
                "-m", "pip", "install", "--upgrade", "pip"
            )
        }

        exec {
            commandLine(
                if (OperatingSystem.current().isWindows)
                    "$venvDir/Scripts/pip" else "$venvDir/bin/pip",
                "install", "uv==0.2.0"
            )
        }
    }
}

// 安装项目依赖
tasks.register("installDependencies") {
    group = "Build"
    description = "Install Python dependencies"

    dependsOn("installUv")

    inputs.file("requirements.txt")
    outputs.dir("$buildDir/dist-packages")

    doLast {
        exec {
            commandLine(
                uvExecutable,
                "pip", "install", "-r", "requirements.txt"
            )
            environment("PYTHONPATH", "$buildDir/dist-packages")
        }
    }
}

// 生成锁定文件
tasks.register("generateLockfile") {
    group = "Build"
    description = "Generate dependency lockfile"

    dependsOn("installUv")

    inputs.file("requirements.txt")
    outputs.file("requirements.lock")

    doLast {
        exec {
            commandLine(
                uvExecutable,
                "pip", "compile", "requirements.txt", "-o", "requirements.lock"
            )
        }
    }
}

// 运行 Python 测试 (避免与内置 test 任务冲突)
tasks.register("runPythonTests") {
    group = "Verification"
    description = "Run Python tests"

    dependsOn("installDependencies")

    doLast {
        exec {
            commandLine(
                uvExecutable,
                "run", "pytest", "tests/"
            )
            environment("PYTHONPATH", "$buildDir/dist-packages")
        }
    }
}

// 运行代码检查
tasks.register("runPythonLint") {
    group = "Verification"
    description = "Run Python code linting"

    dependsOn("installDependencies")

    doLast {
        exec {
            commandLine(
                uvExecutable,
                "run", "ruff", "check", "src/"
            )
        }
    }
}

// 安全扫描
tasks.register("runPythonSecurityScan") {
    group = "Verification"
    description = "Run Python security scan"

    dependsOn("installDependencies")

    doLast {
        exec {
            commandLine(
                uvExecutable,
                "run", "bandit", "-r", "src/"
            )
        }
    }
}

// 构建应用包
tasks.register("buildPython") {
    group = "Build"
    description = "Build Python application package"

    dependsOn("installDependencies", "generateLockfile")

    doLast {
        // 复制源文件到构建目录
        copy {
            from("src")
            into("$buildDir/dist")
        }

        // 复制依赖清单
        copy {
            from("requirements.lock")
            into("$buildDir/dist")
        }
    }

    outputs.dir("$buildDir/dist")
}

// 运行应用
tasks.register("runPythonApp") {
    group = "Application"
    description = "Run Python application"

    dependsOn("installDependencies")

    doLast {
        exec {
            commandLine(
                uvExecutable,
                "run", "python", "src/main.py"
            )
            environment("PYTHONPATH", "$buildDir/dist-packages")
        }
    }
}

// 容器构建
tasks.register("buildDockerImage") {
    group = "Build"
    description = "Build Docker image"

    dependsOn("generateLockfile")

    inputs.file("Dockerfile")
    outputs.dir("$buildDir/docker")

    doLast {
        exec {
            commandLine("docker", "build", "-t", "$group/${project.name}:$version", ".")
        }
    }
}

// 清理任务
tasks.named("clean") {
    group = "Build"
    description = "Clean build artifacts"

    doLast {
        delete(buildDir)
        delete("__pycache__")
        delete("**/*.pyc")
        delete("dist")
    }
}

// 任务依赖配置
tasks.register("pythonCheck") {
    group = "Verification"
    description = "Run all Python quality checks"

    dependsOn("runPythonTests", "runPythonLint", "runPythonSecurityScan")
}

tasks.register("assemblePython") {
    group = "Build"
    description = "Assemble Python artifacts"

    dependsOn("buildPython", "buildDockerImage")
}

// 检查任务配置
tasks.register("checkEnvironment") {
    group = "Verification"
    description = "Check required environment variables"

    doLast {
        val requiredVars = listOf("DB_URL", "API_KEY")
        val missing = mutableListOf<String>()

        requiredVars.forEach { varName ->
            if (System.getenv(varName) == null) {
                missing.add(varName)
            }
        }

        if (missing.isNotEmpty()) {
            throw GradleException("Missing required environment variables: ${missing.joinToString(", ")}")
        }
    }
}

// 设置测试依赖
tasks.named("runPythonTests") {
    dependsOn("checkEnvironment")
}

// 创建发布任务
tasks.register("publishPython") {
    group = "Publishing"
    description = "Publish Python artifacts"

    dependsOn("buildPython", "buildDockerImage")

    doLast {
        println("Publishing Python artifacts to repository...")
        // 添加实际的发布逻辑
    }
}

// 跨平台支持增强
tasks.withType<Exec>().configureEach {
    if (OperatingSystem.current().isWindows) {
        // Windows 命令处理
        commandLine = listOf("cmd", "/c") + commandLine
    }
}

// 依赖扫描
tasks.register("scanPythonDependencies") {
    group = "Verification"
    description = "Scan Python dependencies for vulnerabilities"

    dependsOn("generateLockfile")

    doLast {
        exec {
            commandLine(
                uvExecutable,
                "pip", "install", "safety"
            )
        }

        exec {
            commandLine(
                uvExecutable,
                "run", "safety", "check", "-r", "requirements.lock"
            )
        }
    }
}

// 环境特定配置
tasks.register("setProductionEnv") {
    group = "Configuration"
    description = "Set production environment variables"

    doLast {
        file("$buildDir/.env").writeText("""
            APP_ENV=production
            LOG_LEVEL=WARNING
            DATABASE_URL=${System.getenv("PROD_DB_URL")}
        """.trimIndent())
    }
}

tasks.named("buildDockerImage") {
    dependsOn("setProductionEnv")

    doLast {
        exec {
            commandLine("docker", "build", "--build-arg", "ENV_FILE=$buildDir/.env", "...")
        }
    }
}

// 主构建任务
tasks.named("build") {
    dependsOn("buildPython")
}

tasks.named("check") {
    dependsOn("pythonCheck")
}

tasks.named("assemble") {
    dependsOn("assemblePython")
}