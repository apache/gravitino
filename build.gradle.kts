/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
import com.github.gradle.node.NodeExtension
import com.github.gradle.node.NodePlugin
import com.github.jk1.license.filter.DependencyFilter
import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.render.InventoryHtmlReportRenderer
import com.github.jk1.license.render.ReportRenderer
import com.github.vlsi.gradle.dsl.configureEach
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.internal.hash.ChecksumService
import org.gradle.kotlin.dsl.support.serviceOf
import java.io.File
import java.util.Locale

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("jacoco")
  alias(libs.plugins.gradle.extensions)
  alias(libs.plugins.node) apply false

  // Spotless version < 6.19.0 (https://github.com/diffplug/spotless/issues/1819) has an issue
  // running against JDK21, but we cannot upgrade the spotless to 6.19.0 or later since it only
  // support JDK11+. So we don't support JDK21 and thrown an exception for now.
  if (JavaVersion.current() >= JavaVersion.VERSION_1_8 &&
    JavaVersion.current() <= JavaVersion.VERSION_17
  ) {
    alias(libs.plugins.spotless)
  } else {
    throw GradleException(
      "Gravitino Gradle current doesn't support " +
        "Java version: ${JavaVersion.current()}. Please use JDK8 to 17."
    )
  }

  alias(libs.plugins.publish)
  // Apply one top level rat plugin to perform any required license enforcement analysis
  alias(libs.plugins.rat)
  id("com.github.jk1.dependency-license-report") version "2.5"
  id("org.cyclonedx.bom") version "1.5.0" // Newer version fail due to our setup
}

licenseReport {
  renderers = arrayOf<ReportRenderer>(InventoryHtmlReportRenderer("report.html", "Backend"))
  filters = arrayOf<DependencyFilter>(LicenseBundleNormalizer())
}
repositories { mavenCentral() }

allprojects {
  apply(plugin = "com.diffplug.spotless")

  repositories {
    mavenCentral()
    mavenLocal()
  }

  plugins.withType<com.diffplug.gradle.spotless.SpotlessPlugin>().configureEach {
    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
      java {
        // Fix the Google Java Format version to 1.7. Since JDK8 only support Google Java Format
        // 1.7, which is not compatible with JDK17. We will use a newer version when we upgrade to
        // JDK17.
        googleJavaFormat("1.7")
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

      kotlinGradle {
        target("*.gradle.kts")
        ktlint().editorConfigOverride(mapOf("indent_size" to 2))
      }
    }
  }
}

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

  java {
    toolchain {
      if (project.name == "trino-connector") {
        languageVersion.set(JavaLanguageVersion.of(17))
      } else {
        languageVersion.set(JavaLanguageVersion.of(8))
      }
    }
  }

  gradle.projectsEvaluated {
    tasks.withType<JavaCompile> {
      options.compilerArgs.addAll(arrayOf("-Xlint:deprecation", "-Werror"))
    }
  }

  val sourcesJar by tasks.registering(Jar::class) {
    from(sourceSets.named("main").get().allSource)
    archiveClassifier.set("sources")
  }

  val javadocJar by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
    from(tasks["javadoc"])
  }

  if (project.name in listOf("web", "docs")) {
    plugins.apply(NodePlugin::class)
    configure<NodeExtension> {
      version.set("20.9.0")
      npmVersion.set("10.1.0")
      yarnVersion.set("1.22.19")
      nodeProjectDir.set(file("$rootDir/.node"))
      download.set(true)
    }
  }

  apply(plugin = "signing")
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

  configure<SigningExtension> {
    val gpgId = System.getenv("GPG_ID")
    val gpgSecretKey = System.getenv("GPG_PRIVATE_KEY")
    val gpgKeyPassword = System.getenv("GPG_PASSPHRASE")
    useInMemoryPgpKeys(gpgId, gpgSecretKey, gpgKeyPassword)
    sign(publishing.publications)
  }

  tasks.configureEach<Test> {
    testLogging {
      exceptionFormat = TestExceptionFormat.FULL
      showExceptions = true
      showCauses = true
      showStackTraces = true
    }
    reports.html.outputLocation.set(file("${rootProject.projectDir}/build/reports/"))
    val skipTests = project.hasProperty("skipTests")
    if (!skipTests) {
      if (project.name == "trino-connector") {
        useTestNG()
        maxHeapSize = "2G"
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

  tasks.register("allDeps", DependencyReportTask::class)

  group = "com.datastrato.gravitino"
  version = "$version"

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
}

tasks.rat {
  substringMatcher("DS", "Datastrato", "Copyright 2023 Datastrato.")
  approvedLicense("Datastrato")
  approvedLicense("Apache License Version 2.0")

  // Set input directory to that of the root project instead of the CWD. This
  // makes .gitignore rules (added below) work properly.
  inputDir.set(project.rootDir)

  val exclusions = mutableListOf(
    // Ignore files we track but do not need headers
    "**/.github/**/*",
    "dev/docker/**/*.xml",
    "**/*.log",
    "**/licenses/*.txt",
    "**/licenses/*.md",
    "integration-test/**",
    "web/.**",
    "web/dist/**/*",
    "web/node_modules/**/*",
    "web/src/iconify-bundle/bundle-icons-react.js",
    "web/src/iconify-bundle/icons-bundle-react.js",
    "web/yarn.lock",
    "**/LICENSE.*",
    "**/NOTICE.*"
  )

  // Add .gitignore excludes to the Apache Rat exclusion list.
  val gitIgnore = project(":").file(".gitignore")
  if (gitIgnore.exists()) {
    val gitIgnoreExcludes = gitIgnore.readLines().filter {
      it.isNotEmpty() && !it.startsWith("#")
    }
    exclusions.addAll(gitIgnoreExcludes)
  }

  dependsOn(":web:nodeSetup")

  verbose.set(true)
  failOnError.set(true)
  setExcludes(exclusions)
}

tasks.check.get().dependsOn(tasks.rat)

tasks.cyclonedxBom {
  setIncludeConfigs(listOf("runtimeClasspath"))
}

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
        from(projectDir.dir("web/build/libs/${rootProject.name}-web-$version.war")) { into("package/web") }
        into(outputDir)
        rename { fileName ->
          fileName.replace(".template", "")
        }
        fileMode = 0b111101101
      }
      copy {
        from(projectDir.dir("licenses")) { into("package/licenses") }
        from(projectDir.file("LICENSE.bin")) { into("package") }
        from(projectDir.file("NOTICE.bin")) { into("package") }
        from(projectDir.file("README.md")) { into("package") }
        into(outputDir)
        rename { fileName ->
          fileName.replace(".bin", "")
        }
      }

      // Create the directory 'data' for storage.
      val directory = File("distribution/package/data")
      directory.mkdirs()
    }
  }

  val assembleDistribution by registering(Tar::class) {
    dependsOn("assembleTrinoConnector")
    group = "gravitino distribution"
    finalizedBy("checksumDistribution")
    into("${rootProject.name}-$version-bin")
    from(compileDistribution.map { it.outputs.files.single() })
    compression = Compression.GZIP
    archiveFileName.set("${rootProject.name}-$version-bin.tar.gz")
    destinationDirectory.set(projectDir.dir("distribution"))
  }

  val assembleTrinoConnector by registering(Tar::class) {
    dependsOn("trino-connector:copyLibs")
    group = "gravitino distribution"
    finalizedBy("checksumTrinoConnector")
    into("${rootProject.name}-trino-connector-$version")
    from("trino-connector/build/libs")
    compression = Compression.GZIP
    archiveFileName.set("${rootProject.name}-trino-connector-$version.tar.gz")
    destinationDirectory.set(projectDir.dir("distribution"))
  }

  register("checksumDistribution") {
    group = "gravitino distribution"
    dependsOn(assembleDistribution, "checksumTrinoConnector")
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

  register("checksumTrinoConnector") {
    group = "gravitino distribution"
    dependsOn(assembleTrinoConnector)
    val archiveFile = assembleTrinoConnector.flatMap { it.archiveFile }
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

  register("copySubprojectDependencies", Copy::class) {
    subprojects.forEach() {
      if (!it.name.startsWith("catalog") &&
        !it.name.startsWith("client") && it.name != "trino-connector" &&
        it.name != "integration-test"
      ) {
        from(it.configurations.runtimeClasspath)
        into("distribution/package/libs")
      }
    }
  }

  register("copySubprojectLib", Copy::class) {
    subprojects.forEach() {
      if (!it.name.startsWith("catalog") &&
        !it.name.startsWith("client") &&
        it.name != "trino-connector" &&
        it.name != "integration-test"
      ) {
        dependsOn("${it.name}:build")
        from("${it.name}/build/libs")
        into("distribution/package/libs")
        include("*.jar")
        setDuplicatesStrategy(DuplicatesStrategy.INCLUDE)
      }
    }
  }

  register("copyCatalogLibAndConfigs", Copy::class) {
    dependsOn(
      ":catalogs:catalog-hive:copyLibAndConfig",
      ":catalogs:catalog-lakehouse-iceberg:copyLibAndConfig",
      ":catalogs:catalog-jdbc-mysql:copyLibAndConfig",
      ":catalogs:catalog-jdbc-postgresql:copyLibAndConfig"
    )
  }

  clean {
    dependsOn(cleanDistribution)
  }
}
