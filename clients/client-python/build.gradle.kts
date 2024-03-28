/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import org.gradle.internal.os.OperatingSystem
import java.io.IOException

tasks.withType(Exec::class) {
  workingDir = file("${project.projectDir}")
}

val os = OperatingSystem.current()
val pythonExecutable = if (os.isWindows) {
  if (execOrNull("where python") != null) {
    "python"
  } else if (execOrNull("where python3") != null) {
    "python3"
  } else {
    throw IllegalStateException("Python not found, please install it first...")
  }
} else {
  if (execOrNull("which python") != null) {
    "python"
  } else if (execOrNull("which python3") != null) {
    "python3"
  } else {
    throw IllegalStateException("Python not found, please install it first...")
  }
}
println(": $pythonExecutable")

fun execOrNull(command: String): String? {
  return try {
    val parts = command.split("\\s".toRegex())
    val proc = ProcessBuilder(*parts.toTypedArray())
      .redirectOutput(ProcessBuilder.Redirect.PIPE)
      .redirectError(ProcessBuilder.Redirect.PIPE)
      .start()

    proc.waitFor(1, TimeUnit.MINUTES)
    if (proc.exitValue() != 0) {
      null
    } else {
      proc.inputStream.bufferedReader().readText().trim()
    }
  } catch (e: IOException) {
    null
  }
}

tasks {
  val pythonVersion by registering(Exec::class) {
    commandLine("bash", "-c", "$pythonExecutable --version")
  }

  val installPip by registering(Exec::class) {
    dependsOn(pythonVersion)
    commandLine("bash", "-c", "$pythonExecutable -m pip install --upgrade pip")
  }

  val installDeps by registering(Exec::class) {
    dependsOn(installPip)
    commandLine("bash", "-c", "$pythonExecutable -m pip install -r requirements.txt")
  }

  val pyTest by registering(Exec::class) {
    dependsOn(installDeps)
    commandLine("bash", "-c", "$pythonExecutable -m pytest ${project.projectDir}/tests")
  }

  test {
    dependsOn(pyTest)
  }

  clean {
    delete("build")
  }
}
