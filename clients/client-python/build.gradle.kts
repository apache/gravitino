/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

tasks.withType(Exec::class) {
  workingDir = file("${project.projectDir}")
}

tasks {
  val pythonVersion by registering(Exec::class) {
    commandLine("python", "--version")
  }

  val installPip by registering(Exec::class) {
    dependsOn(pythonVersion)
    commandLine("python", "-m", "pip", "install", "--upgrade", "pip")
  }

  val installDeps by registering(Exec::class) {
    dependsOn(installPip)
    commandLine("python", "-m", "pip", "install", "-r", "requirements.txt")
  }

  val pyTest by registering(Exec::class) {
    dependsOn(installDeps)
    commandLine("pytest", "${project.projectDir}/tests")
  }

  test {
    dependsOn(pyTest)
  }

  clean {
    delete("build")
  }
}
