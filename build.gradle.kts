/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach
import java.util.Locale
import org.gradle.internal.hash.ChecksumService
import org.gradle.kotlin.dsl.support.serviceOf

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("jacoco")
  alias(libs.plugins.gradle.extensions)
  alias(libs.plugins.spotless)
  alias(libs.plugins.publish)
  // Apply one top level rat plugin to perform any required license enforcement analysis
  alias(libs.plugins.rat)
}

repositories { mavenCentral() }

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(8))
    withJavadocJar()
    withSourcesJar()
  }
}

subprojects {
  apply(plugin = "jacoco")

  repositories {
    mavenCentral()
    mavenLocal()
  }

  tasks.configureEach<Test> {
    val skipTests = project.hasProperty("skipTests")
    if (!skipTests) {
      useJUnitPlatform()
      finalizedBy(tasks.getByName("jacocoTestReport"))
    }
  }

  tasks.withType<JacocoReport> {
    reports {
      csv.required.set(true)
      xml.required.set(true)
      html.required.set(true)
    }
  }

  val allDeps by tasks.registering(DependencyReportTask::class)

  group = "com.datastrato.graviton"
  version = "${version}"

  tasks.withType<Jar> {
    archiveBaseName.set("${rootProject.name.lowercase(Locale.getDefault())}-${project.name}")
    if (project.name == "server") {
      from(sourceSets.main.get().resources)
      setDuplicatesStrategy(DuplicatesStrategy.INCLUDE)
    }

    if (project.name != "integration-test") {
      exclude("log4j2.properties")
      exclude("test/**")
    }
  }

  plugins.withType<SpotlessPlugin>().configureEach {
    configure<SpotlessExtension> {
      java {
        googleJavaFormat()
        removeUnusedImports()
        trimTrailingWhitespace()
        replaceRegex(
          "Remove wildcard imports",
          "import\\s+[^\\*\\s]+\\*;(\\r\\n|\\r|\\n)",
          "$1"
        )
        replaceRegex(
          "Remove static wildcard imports",
          "import\\s+(?:static\\s+)?[^*\\s]+\\*;(\\r\\n|\\r|\\n)",
          "$1"
        )

        targetExclude("**/build/**")
      }
    }
  }
}

nexusPublishing {
  repositories {
    create("sonatype") {
      val sonatypeUser =
        System.getenv("SONATYPE_USER").takeUnless { it.isNullOrEmpty() }
          ?: extra["SONATYPE_USER"].toString()
      val sonatypePassword =
        System.getenv("SONATYPE_PASSWORD").takeUnless { it.isNullOrEmpty() }
          ?: extra["SONATYPE_PASSWORD"].toString()
      nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))

      snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
      username.set(sonatypeUser)
      password.set(sonatypePassword)
    }
  }
}

tasks.rat {
  substringMatcher("DS", "Datastrato", "Copyright 2023 Datastrato.")
  approvedLicense("Datastrato")
  approvedLicense("Apache License Version 2.0")

  // Set input directory to that of the root project instead of the CWD. This
  // makes .gitignore rules (added below) work properly.
  inputDir.set(project.rootDir)

  val exclusions = mutableListOf(
    // Ignore files we track but do not distribute
    "**/.github/**/*",
    "dev/docker/**/*.xml",
  )

  // Add .gitignore excludes to the Apache Rat exclusion list.
  val gitIgnore = project(":").file(".gitignore")
  if (gitIgnore.exists()) {
    val gitIgnoreExcludes = gitIgnore.readLines().filter {
      it.isNotEmpty() && !it.startsWith("#")
    }
    exclusions.addAll(gitIgnoreExcludes)
  }

  verbose.set(true)
  failOnError.set(true)
  setExcludes(exclusions)
}
tasks.check.get().dependsOn(tasks.rat)

jacoco {
  toolVersion = "0.8.10"
  reportsDirectory.set(layout.buildDirectory.dir("JacocoReport"))
}

tasks {
  val projectDir = layout.projectDirectory
  val outputDir = projectDir.dir("distribution")

  val compileDistribution by registering {
    dependsOn("copySubprojectDependencies", "copyCatalogLibs", "copySubprojectLib")

    group = "graviton distribution"
    outputs.dir(projectDir.dir("distribution/package"))
    doLast {
      copy {
        from(projectDir.dir("conf")) { into("package/conf") }
        from(projectDir.dir("bin")) { into("package/bin") }
        into(outputDir)
        rename { fileName ->
          fileName.replace(".template", "")
        }
        fileMode = 0b111101101
      }
    }
  }

  val assembleDistribution by registering(Tar::class) {
    group = "graviton distribution"
    finalizedBy("checksumDistribution")
    from(compileDistribution.map { it.outputs.files.single() })
    compression = Compression.GZIP
    archiveFileName.set("${rootProject.name}-${version}.tar.gz")
    destinationDirectory.set(projectDir.dir("distribution"))
  }

  register("checksumDistribution") {
    group = "graviton distribution"
    dependsOn(assembleDistribution)
    val archiveFile = assembleDistribution.flatMap { it.archiveFile }
    val checksumFile = archiveFile.map { archive ->
      archive.asFile.let { it.resolveSibling("${it.name}.sha256") }
    }
    inputs.file(archiveFile)
    outputs.file(checksumFile)
    doLast {
      checksumFile.get().writeText(
        serviceOf<ChecksumService>().sha256(archiveFile.get().asFile).toString()
      )
    }
  }

  val cleanDistribution by registering(Delete::class) {
    group = "graviton distribution"
    delete(outputDir)
  }

  val copySubprojectDependencies by registering(Copy::class) {
    subprojects.forEach() {
      if (!it.name.startsWith("catalog")
          && !it.name.startsWith("client")
          && it.name != "integration-test") {
        from(it.configurations.runtimeClasspath)
        into("distribution/package/libs")
      }
    }
  }

  val copySubprojectLib by registering(Copy::class) {
    subprojects.forEach() {
      if (!it.name.startsWith("catalog")
          && !it.name.startsWith("client")
          && it.name != "integration-test") {
        dependsOn("${it.name}:build")
        from("${it.name}/build/libs")
        into("distribution/package/libs")
        include("*.jar")
        setDuplicatesStrategy(DuplicatesStrategy.INCLUDE)
      }
    }
  }

  val copyCatalogLibs by registering(Copy::class) {
    dependsOn(":catalogs:catalog-hive:copyCatalogLibs",
            ":catalogs:catalog-lakehouse-iceberg:copyCatalogLibs")
  }

  clean {
    dependsOn(cleanDistribution)
    delete("/tmp/graviton")
    delete("server/src/main/resources/project.properties")
  }
}
