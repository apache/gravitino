/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach
import java.util.Locale
import java.io.File
import org.gradle.internal.hash.ChecksumService
import org.gradle.kotlin.dsl.support.serviceOf
import com.github.jk1.license.render.ReportRenderer
import com.github.jk1.license.render.InventoryHtmlReportRenderer
import com.github.jk1.license.filter.DependencyFilter
import com.github.jk1.license.filter.LicenseBundleNormalizer

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("jacoco")
  alias(libs.plugins.gradle.extensions)
  alias(libs.plugins.spotless)
  alias(libs.plugins.publish)
  id("org.jetbrains.dokka") version "1.9.10"
  // Apply one top level rat plugin to perform any required license enforcement analysis
  alias(libs.plugins.rat)
  id("com.github.jk1.dependency-license-report") version "2.5"
}

licenseReport {
    renderers = arrayOf<ReportRenderer>(InventoryHtmlReportRenderer("report.html", "Backend"))
    filters = arrayOf<DependencyFilter>(LicenseBundleNormalizer())
}
repositories { mavenCentral() }

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(8))
    withJavadocJar()
    withSourcesJar()
  }
}

allprojects {
  repositories {
    mavenCentral()
    mavenLocal()
  }

  tasks.withType<org.jetbrains.dokka.gradle.DokkaTask>().configureEach {
    dokkaSourceSets {
      configureEach {
        reportUndocumented.set(false)
      }
    }
  }
}

apply(plugin = "io.github.gradle-nexus.publish-plugin")

nexusPublishing {
  repositories {
    sonatype {
      nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
      snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))

      val sonatypeUser =
              System.getenv("SONATYPE_USER").takeUnless { it.isNullOrEmpty() }
                      ?: extra["SONATYPE_USER"].toString()
      val sonatypePassword =
              System.getenv("SONATYPE_PASSWORD").takeUnless { it.isNullOrEmpty() }
                      ?: extra["SONATYPE_PASSWORD"].toString()

      username.set(sonatypeUser)
      password.set(sonatypePassword)
    }
  }

  packageGroup.set("com.datastrato.gravitino")
}

dependencies {
  testImplementation(libs.testng)
}

subprojects {
  apply(plugin = "jacoco")
  apply(plugin = "maven-publish")
  apply(plugin = "java")

  repositories {
    mavenCentral()
    mavenLocal()
  }

  val sourcesJar by tasks.registering(Jar::class) {
    from(sourceSets.named("main").get().allSource)
    archiveClassifier.set("sources")
  }

  val javadocJar by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
    from(tasks["javadoc"])
  }


  publishing {
    publications {
      create<MavenPublication>("MavenJava") {
        from(components["java"])
        artifact(sourcesJar)
        artifact(javadocJar)

        pom {
          name.set("Gravitino")
          description.set("Gravitino is a high-performance, geo-distributed and federated metadata lake.")
          url.set("https://datastrato.ai")
          licenses {
            license {
              name.set("The Apache Software License, Version 2.0")
              url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
            }
          }
          developers {
            developer {
              id.set("The maintainers of Gravitino")
              name.set("support")
              email.set("support@datastrato.com")
            }
          }
          scm {
            url.set("https://github.com/datastrato/gravitino")
            connection.set("scm:git:git://github.com/datastrato/gravitino.git")
          }
        }
      }
    }
  }

  tasks.configureEach<Test> {
    val skipTests = project.hasProperty("skipTests")
    if (!skipTests) {
      if (project.name == "trino-connector") {
        useTestNG()
      } else {
        useJUnitPlatform()
      }
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

  group = "com.datastrato.gravitino"
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
    "**/*.log",
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
    dependsOn("copySubprojectDependencies", "copyCatalogLibAndConfigs", "copySubprojectLib")

    group = "gravitino distribution"
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

      // Create the directory 'data' for storage.
      val directory = File("distribution/package/data")
      directory.mkdirs()
    }
  }

  val assembleDistribution by registering(Tar::class) {
    group = "gravitino distribution"
    finalizedBy("checksumDistribution")
    into("${rootProject.name}-${version}")
    from(compileDistribution.map { it.outputs.files.single() })
    compression = Compression.GZIP
    archiveFileName.set("${rootProject.name}-${version}.tar.gz")
    destinationDirectory.set(projectDir.dir("distribution"))
  }

  register("checksumDistribution") {
    group = "gravitino distribution"
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
    group = "gravitino distribution"
    delete(outputDir)
  }

  val copySubprojectDependencies by registering(Copy::class) {
    subprojects.forEach() {
      if (!it.name.startsWith("catalog")
          && !it.name.startsWith("client") 
          && it.name != "trino-connector"
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
          && it.name != "trino-connector"
          && it.name != "integration-test") {
        dependsOn("${it.name}:build")
        from("${it.name}/build/libs")
        into("distribution/package/libs")
        include("*.jar")
        setDuplicatesStrategy(DuplicatesStrategy.INCLUDE)
      }
    }
  }

  val copyCatalogLibAndConfigs by registering(Copy::class) {
    dependsOn(":catalogs:catalog-hive:copyLibAndConfig",
            ":catalogs:catalog-lakehouse-iceberg:copyLibAndConfig")
  }

  clean {
    dependsOn(cleanDistribution)
    delete("server/src/main/resources/project.properties")
  }
}
