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
import com.github.gradle.node.NodeExtension
import com.github.gradle.node.NodePlugin
import com.github.jk1.license.filter.DependencyFilter
import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.render.InventoryHtmlReportRenderer
import com.github.jk1.license.render.ReportRenderer
import com.github.vlsi.gradle.dsl.configureEach
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.internal.hash.ChecksumService
import org.gradle.internal.os.OperatingSystem
import org.gradle.kotlin.dsl.support.serviceOf
import java.io.IOException
import java.util.Locale

Locale.setDefault(Locale.US)

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
      "Gravitino Gradle toolchain current doesn't support " +
        "Java version: ${JavaVersion.current()}. Please use JDK8 to 17."
    )
  }

  alias(libs.plugins.publish)
  // Apply one top level rat plugin to perform any required license enforcement analysis
  alias(libs.plugins.rat)
  alias(libs.plugins.bom)
  alias(libs.plugins.dependencyLicenseReport)
  alias(libs.plugins.tasktree)
  alias(libs.plugins.errorprone)
}

if (extra["jdkVersion"] !in listOf("8", "11", "17")) {
  throw GradleException(
    "Gravitino current doesn't support building with " +
      "Java version: ${extra["jdkVersion"]}. Please use JDK8, 11 or 17."
  )
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
if (scalaVersion !in listOf("2.12", "2.13")) {
  throw GradleException("Found unsupported Scala version: $scalaVersion")
}

project.extra["extraJvmArgs"] = if (extra["jdkVersion"] in listOf("8", "11")) {
  listOf()
} else {
  listOf(
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens", "java.base/java.io=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang=ALL-UNNAMED",
    "--add-opens", "java.base/java.math=ALL-UNNAMED",
    "--add-opens", "java.base/java.net=ALL-UNNAMED",
    "--add-opens", "java.base/java.nio=ALL-UNNAMED",
    "--add-opens", "java.base/java.text=ALL-UNNAMED",
    "--add-opens", "java.base/java.time=ALL-UNNAMED",
    "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens", "java.base/java.util.regex=ALL-UNNAMED",
    "--add-opens", "java.base/java.util=ALL-UNNAMED",
    "--add-opens", "java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens", "java.base/jdk.internal.reflect=ALL-UNNAMED",
    "--add-opens", "java.sql/java.sql=ALL-UNNAMED",
    "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens", "java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED",
    "--add-opens", "java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED"
  )
}

val pythonVersion: String = project.properties["pythonVersion"] as? String ?: project.extra["pythonVersion"].toString()
project.extra["pythonVersion"] = pythonVersion

licenseReport {
  renderers = arrayOf<ReportRenderer>(InventoryHtmlReportRenderer("report.html", "Backend"))
  filters = arrayOf<DependencyFilter>(LicenseBundleNormalizer())
}
repositories { mavenCentral() }

allprojects {
  // Gravitino Python client project didn't need to apply the Spotless plugin
  if (project.name == "client-python") {
    return@allprojects
  }

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

        targetExclude("**/build/**", "**/.pnpm/***")
      }

      kotlinGradle {
        target("*.gradle.kts")
        ktlint().editorConfigOverride(mapOf("indent_size" to 2))
      }
    }
  }

  val setTestEnvironment: (Test) -> Unit = { param ->
    param.doFirst {
      param.jvmArgs(project.property("extraJvmArgs") as List<*>)

      // Default use MiniGravitino to run integration tests
      param.environment("GRAVITINO_ROOT_DIR", project.rootDir.path)
      param.environment("IT_PROJECT_DIR", project.buildDir.path)
      // If the environment variable `HADOOP_USER_NAME` is not customized in submodule,
      // then set it to "anonymous"
      if (param.environment["HADOOP_USER_NAME"] == null) {
        param.environment("HADOOP_USER_NAME", "anonymous")
      }
      param.environment("HADOOP_HOME", "/tmp")
      param.environment("PROJECT_VERSION", project.version)

      // Gravitino CI Docker image
      param.environment("GRAVITINO_CI_HIVE_DOCKER_IMAGE", "apache/gravitino-ci:hive-0.1.17")
      param.environment("GRAVITINO_CI_KERBEROS_HIVE_DOCKER_IMAGE", "apache/gravitino-ci:kerberos-hive-0.1.5")
      param.environment("GRAVITINO_CI_DORIS_DOCKER_IMAGE", "apache/gravitino-ci:doris-0.1.5")
      param.environment("GRAVITINO_CI_TRINO_DOCKER_IMAGE", "apache/gravitino-ci:trino-0.1.6")
      param.environment("GRAVITINO_CI_RANGER_DOCKER_IMAGE", "apache/gravitino-ci:ranger-0.1.1")
      param.environment("GRAVITINO_CI_KAFKA_DOCKER_IMAGE", "apache/kafka:3.7.0")
      param.environment("GRAVITINO_CI_LOCALSTACK_DOCKER_IMAGE", "localstack/localstack:latest")

      val dockerRunning = project.rootProject.extra["dockerRunning"] as? Boolean ?: false
      val macDockerConnector = project.rootProject.extra["macDockerConnector"] as? Boolean ?: false
      if (OperatingSystem.current().isMacOsX() &&
        dockerRunning &&
        macDockerConnector
      ) {
        param.environment("NEED_CREATE_DOCKER_NETWORK", "true")
      }

      val icebergVersion: String = libs.versions.iceberg.get()
      param.systemProperty("ICEBERG_VERSION", icebergVersion)

      // Change poll image pause time from 30s to 60s
      param.environment("TESTCONTAINERS_PULL_PAUSE_TIMEOUT", "60")
      val jdbcDatabase = project.properties["jdbcBackend"] as? String ?: "h2"
      param.environment("jdbcBackend", jdbcDatabase)

      val testMode = project.properties["testMode"] as? String ?: "embedded"
      param.systemProperty("gravitino.log.path", "build/${project.name}-integration-test.log")
      project.delete("build/${project.name}-integration-test.log")
      if (testMode == "deploy") {
        param.environment("GRAVITINO_HOME", project.rootDir.path + "/distribution/package")
        param.systemProperty("testMode", "deploy")
      } else if (testMode == "embedded") {
        param.environment("GRAVITINO_HOME", project.rootDir.path)
        param.environment("GRAVITINO_TEST", "true")
        param.environment("GRAVITINO_WAR", project.rootDir.path + "/web/web/dist/")
        param.systemProperty("testMode", "embedded")
      } else {
        throw GradleException("Gravitino integration tests only support [-PtestMode=embedded] or [-PtestMode=deploy] mode!")
      }

      param.useJUnitPlatform()
      val skipUTs = project.hasProperty("skipTests")
      if (skipUTs) {
        // Only run integration tests
        param.include("**/integration/test/**")
      }

      param.useJUnitPlatform {
        val dockerTest = project.rootProject.extra["dockerTest"] as? Boolean ?: false
        if (!dockerTest) {
          excludeTags("gravitino-docker-test")
        }
      }
    }
  }

  extra["initTestParam"] = setTestEnvironment
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

  packageGroup.set("org.apache.gravitino")
}

subprojects {
  // Gravitino Python client project didn't need to apply the java plugin
  if (project.name == "client-python") {
    return@subprojects
  }

  apply(plugin = "jacoco")
  apply(plugin = "maven-publish")
  apply(plugin = "java")

  repositories {
    mavenCentral()
    mavenLocal()
  }

  java {
    toolchain {
      // Some JDK vendors like Homebrew installed OpenJDK 17 have problems in building trino-connector:
      // It will cause tests of Trino-connector hanging forever on macOS, to avoid this issue and
      // other vendor-related problems, Gravitino will use the specified AMAZON OpenJDK 17 to build
      // Trino-connector on macOS.
      if (project.name == "trino-connector") {
        if (OperatingSystem.current().isMacOsX) {
          vendor.set(JvmVendorSpec.AMAZON)
        }
        languageVersion.set(JavaLanguageVersion.of(17))
      } else {
        languageVersion.set(JavaLanguageVersion.of(extra["jdkVersion"].toString().toInt()))
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
      }
    }
  }

  gradle.projectsEvaluated {
    tasks.withType<JavaCompile> {
      options.compilerArgs.addAll(
        arrayOf(
          "-Xlint:cast",
          "-Xlint:deprecation",
          "-Xlint:divzero",
          "-Xlint:empty",
          "-Xlint:fallthrough",
          "-Xlint:finally",
          "-Xlint:overrides",
          "-Xlint:static",
          "-Werror"
        )
      )
    }
  }

  apply(plugin = "net.ltgt.errorprone")
  dependencies {
    errorprone("com.google.errorprone:error_prone_core:2.10.0")
  }

  tasks.withType<JavaCompile>().configureEach {
    options.errorprone.isEnabled.set(true)
    options.errorprone.disableWarningsInGeneratedCode.set(true)
    options.errorprone.disable(
      "AlmostJavadoc",
      "CanonicalDuration",
      "CheckReturnValue",
      "ComparableType",
      "ConstantOverflow",
      "DoubleBraceInitialization",
      "EqualsUnsafeCast",
      "EmptyBlockTag",
      "FutureReturnValueIgnored",
      "InconsistentCapitalization",
      "InconsistentHashCode",
      "JavaTimeDefaultTimeZone",
      "JdkObsolete",
      "LockNotBeforeTry",
      "MissingSummary",
      "MissingOverride",
      "MutableConstantField",
      "NonOverridingEquals",
      "ObjectEqualsForPrimitives",
      "OperatorPrecedence",
      "ReturnValueIgnored",
      "SameNameButDifferent",
      "StaticAssignmentInConstructor",
      "StringSplitter",
      "ThreadPriorityCheck",
      "ThrowIfUncheckedKnownChecked",
      "TypeParameterUnusedInFormals",
      "UnicodeEscape",
      "UnnecessaryParentheses",
      "UnsafeReflectiveConstructionCast",
      "UnusedMethod",
      "VariableNameSameAsType",
      "WaitNotInLoop"
    )
  }

  tasks.withType<Javadoc> {
    options.encoding = "UTF-8"
    options.locale = "en_US"

    val projectName = project.name
    if (projectName == "common" || projectName == "api" || projectName == "client-java" || projectName == "client-cli" || projectName == "filesystem-hadoop3") {
      options {
        (this as CoreJavadocOptions).addStringOption("Xwerror", "-quiet")
        isFailOnError = true
      }
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

  tasks.withType<Jar> {
    into("META-INF") {
      from(rootDir) {
        if (name == "sourcesJar") {
          include("LICENSE")
          include("NOTICE")
        } else {
          include("LICENSE.bin")
          rename("LICENSE.bin", "LICENSE")
          include("NOTICE.bin")
          rename("NOTICE.bin", "NOTICE")
        }
      }
    }
  }

  if (project.name in listOf("web", "docs")) {
    plugins.apply(NodePlugin::class)
    configure<NodeExtension> {
      version.set("20.9.0")
      pnpmVersion.set("9.x")
      nodeProjectDir.set(file("$rootDir/.node"))
      download.set(true)
    }
  }

  apply(plugin = "signing")
  publishing {
    publications {
      create<MavenPublication>("MavenJava") {
        if (project.name == "web" ||
          project.name == "docs" ||
          project.name == "integration-test" ||
          project.name == "integration-test-common"
        ) {
          setArtifacts(emptyList<Any>())
        } else {
          from(components["java"])
          artifact(sourcesJar)
          artifact(javadocJar)
        }

        artifactId = "${rootProject.name.lowercase()}-${project.name}"

        pom {
          name.set("Gravitino")
          description.set("Gravitino is a high-performance, geo-distributed and federated metadata lake.")
          url.set("https://gravitino.apache.org")
          licenses {
            license {
              name.set("The Apache Software License, Version 2.0")
              url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
            }
          }
          developers {
            developer {
              id.set("The Gravitino community")
              name.set("support")
              email.set("dev@gravitino.apache.org")
            }
          }
          scm {
            url.set("https://github.com/apache/gravitino")
            connection.set("scm:git:git://github.com/apache/gravitino.git")
          }
        }
      }
    }
  }

  configure<SigningExtension> {
    val taskNames = gradle.getStartParameter().getTaskNames()
    taskNames.forEach() {
      if (it.contains("publishToMavenLocal")) setRequired(false)
    }

    val gpgId = System.getenv("GPG_ID")
    val gpgSecretKey = System.getenv("GPG_PRIVATE_KEY")
    val gpgKeyPassword = System.getenv("GPG_PASSPHRASE")
    useInMemoryPgpKeys(gpgId, gpgSecretKey, gpgKeyPassword)
    sign(publishing.publications)
  }

  tasks.configureEach<Test> {
    if (project.name != "server-common") {
      val initTest = project.extra.get("initTestParam") as (Test) -> Unit
      initTest(this)
    }

    testLogging {
      exceptionFormat = TestExceptionFormat.FULL
      showExceptions = true
      showCauses = true
      showStackTraces = true
    }
    reports.html.outputLocation.set(file("${rootProject.projectDir}/build/reports/"))
    val skipTests = project.hasProperty("skipTests")
    if (!skipTests) {
      val extraArgs = project.property("extraJvmArgs") as List<String>
      jvmArgs = listOf("-Xmx4G") + extraArgs
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

  tasks.register("allDeps", DependencyReportTask::class)

  group = "org.apache.gravitino"
  version = "$version"

  tasks.withType<Jar> {
    archiveBaseName.set("${rootProject.name.lowercase()}-${project.name}")
    if (project.name == "server") {
      from(sourceSets.main.get().resources)
      setDuplicatesStrategy(DuplicatesStrategy.INCLUDE)
    }

    if (project.name != "integration-test") {
      exclude("log4j2.properties")
      exclude("test/**")
    }
  }
  tasks.named("compileJava").configure {
    dependsOn("spotlessCheck")
  }
}

tasks.rat {
  approvedLicense("Apache License Version 2.0")

  // Set input directory to that of the root project instead of the CWD. This
  // makes .gitignore rules (added below) work properly.
  inputDir.set(project.rootDir)

  val exclusions = mutableListOf(
    // Ignore files we track but do not need full headers
    "**/.github/**/*",
    "dev/docker/**/*.xml",
    "dev/docker/**/*.conf",
    "dev/docker/kerberos-hive/kadm5.acl",
    "**/*.log",
    "**/*.out",
    "**/trino-ci-testset",
    "**/licenses/*.txt",
    "**/licenses/*.md",
    "docs/**/*.md",
    "spark-connector/spark-common/src/test/resources/**",
    "web/web/.**",
    "web/web/next-env.d.ts",
    "web/web/dist/**/*",
    "web/web/node_modules/**/*",
    "web/web/src/lib/utils/axios/**/*",
    "web/web/src/lib/enums/httpEnum.js",
    "web/web/src/types/axios.d.ts",
    "web/web/yarn.lock",
    "web/web/package-lock.json",
    "web/web/pnpm-lock.yaml",
    "web/web/src/lib/icons/svg/**/*.svg",
    "**/LICENSE.*",
    "**/NOTICE.*",
    "DISCLAIMER_WIP.txt",
    "DISCLAIMER.txt",
    "ROADMAP.md",
    "clients/client-python/.pytest_cache/*",
    "clients/client-python/**/__pycache__",
    "clients/client-python/.venv/*",
    "clients/client-python/venv/*",
    "clients/client-python/apache_gravitino.egg-info/*",
    "clients/client-python/gravitino/utils/http_client.py",
    "clients/client-python/tests/unittests/htmlcov/*",
    "clients/client-python/tests/integration/htmlcov/*",
    "clients/client-python/docs/build",
    "clients/client-python/docs/source/generated",
    "clients/cli/src/main/resources/*.txt",
    "clients/filesystem-fuse/Cargo.lock"
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
    dependsOn(":web:web:build", "copySubprojectDependencies", "copyCatalogLibAndConfigs", ":authorizations:copyLibAndConfig", "copySubprojectLib", "iceberg:iceberg-rest-server:copyLibAndConfigs")

    group = "gravitino distribution"
    outputs.dir(projectDir.dir("distribution/package"))
    doLast {
      copy {
        from(projectDir.dir("conf")) { into("package/conf") }
        from(projectDir.dir("bin")) { into("package/bin") }
        from(projectDir.dir("web/web/build/libs/${rootProject.name}-web-$version.war")) { into("package/web") }
        from(projectDir.dir("scripts")) { into("package/scripts") }
        into(outputDir)
        rename { fileName ->
          fileName.replace(".template", "")
        }
        eachFile {
          if (name == "gravitino-env.sh") {
            filter { line ->
              line.replace("GRAVITINO_VERSION_PLACEHOLDER", "$version")
            }
          }
        }
        fileMode = 0b111101101
      }
      copy {
        from(projectDir.dir("licenses")) { into("package/licenses") }
        from(projectDir.file("LICENSE.bin")) { into("package") }
        from(projectDir.file("NOTICE.bin")) { into("package") }
        from(projectDir.file("README.md")) { into("package") }
        from(projectDir.file("DISCLAIMER_WIP.txt")) { into("package") }
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

  val compileIcebergRESTServer by registering {
    dependsOn("iceberg:iceberg-rest-server:copyLibAndConfigsToStandalonePackage")
    group = "gravitino distribution"
    outputs.dir(projectDir.dir("distribution/${rootProject.name}-iceberg-rest-server"))
    doLast {
      copy {
        from(projectDir.dir("conf")) {
          include("${rootProject.name}-iceberg-rest-server.conf.template", "${rootProject.name}-env.sh.template", "log4j2.properties.template")
          into("${rootProject.name}-iceberg-rest-server/conf")
        }
        from(projectDir.dir("bin")) {
          include("common.sh", "${rootProject.name}-iceberg-rest-server.sh")
          into("${rootProject.name}-iceberg-rest-server/bin")
        }
        into(outputDir)
        rename { fileName ->
          fileName.replace(".template", "")
        }
        eachFile {
          if (name == "gravitino-env.sh") {
            filter { line ->
              line.replace("GRAVITINO_VERSION_PLACEHOLDER", "$version")
            }
          }
        }
        fileMode = 0b111101101
      }

      copy {
        from(projectDir.dir("licenses")) { into("${rootProject.name}-iceberg-rest-server/licenses") }
        from(projectDir.file("LICENSE.rest")) { into("${rootProject.name}-iceberg-rest-server") }
        from(projectDir.file("NOTICE.rest")) { into("${rootProject.name}-iceberg-rest-server") }
        from(projectDir.file("README.md")) { into("${rootProject.name}-iceberg-rest-server") }
        from(projectDir.file("DISCLAIMER_WIP.txt")) { into("${rootProject.name}-iceberg-rest-server") }
        into(outputDir)
        rename { fileName ->
          fileName.replace(".rest", "")
        }
      }
    }
  }

  val compileTrinoConnector by registering {
    dependsOn("trino-connector:trino-connector:copyLibs")
    group = "gravitino distribution"
    outputs.dir(projectDir.dir("distribution/${rootProject.name}-trino-connector"))
    doLast {
      copy {
        from(projectDir.dir("licenses")) { into("${rootProject.name}-trino-connector/licenses") }
        from(projectDir.file("LICENSE.trino")) { into("${rootProject.name}-trino-connector") }
        from(projectDir.file("NOTICE.trino")) { into("${rootProject.name}-trino-connector") }
        from(projectDir.file("README.md")) { into("${rootProject.name}-trino-connector") }
        from(projectDir.file("DISCLAIMER_WIP.txt")) { into("${rootProject.name}-trino-connector") }
        into(outputDir)
        rename { fileName ->
          fileName.replace(".trino", "")
        }
      }
    }
  }

  val assembleDistribution by registering(Tar::class) {
    dependsOn("assembleTrinoConnector", "assembleIcebergRESTServer")
    group = "gravitino distribution"
    finalizedBy("checksumDistribution")
    into("${rootProject.name}-$version-bin")
    from(compileDistribution.map { it.outputs.files.single() })
    compression = Compression.GZIP
    archiveFileName.set("${rootProject.name}-$version-bin.tar.gz")
    destinationDirectory.set(projectDir.dir("distribution"))
  }

  val assembleTrinoConnector by registering(Tar::class) {
    dependsOn("compileTrinoConnector")
    group = "gravitino distribution"
    finalizedBy("checksumTrinoConnector")
    into("${rootProject.name}-trino-connector-$version")
    from(compileTrinoConnector.map { it.outputs.files.single() })
    compression = Compression.GZIP
    archiveFileName.set("${rootProject.name}-trino-connector-$version.tar.gz")
    destinationDirectory.set(projectDir.dir("distribution"))
  }

  val assembleIcebergRESTServer by registering(Tar::class) {
    dependsOn("compileIcebergRESTServer")
    group = "gravitino distribution"
    finalizedBy("checksumIcebergRESTServerDistribution")
    into("${rootProject.name}-iceberg-rest-server-$version-bin")
    from(compileIcebergRESTServer.map { it.outputs.files.single() })
    compression = Compression.GZIP
    archiveFileName.set("${rootProject.name}-iceberg-rest-server-$version-bin.tar.gz")
    destinationDirectory.set(projectDir.dir("distribution"))
  }

  register("checksumIcebergRESTServerDistribution") {
    group = "gravitino distribution"
    dependsOn(assembleIcebergRESTServer)
    val archiveFile = assembleIcebergRESTServer.flatMap { it.archiveFile }
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

  register("checksumDistribution") {
    group = "gravitino distribution"
    dependsOn(assembleDistribution, "checksumTrinoConnector", "checksumIcebergRESTServerDistribution")
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
        !it.name.startsWith("authorization") &&
        !it.name.startsWith("cli") &&
        !it.name.startsWith("client") && !it.name.startsWith("filesystem") && !it.name.startsWith("spark") && !it.name.startsWith("iceberg") && it.name != "trino-connector" &&
        it.name != "integration-test" && it.name != "bundled-catalog" && !it.name.startsWith("flink") &&
        it.name != "integration-test" && it.name != "hive-metastore-common" && !it.name.startsWith("flink") &&
        it.parent?.name != "bundles" && it.name != "hadoop-common"
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
        !it.name.startsWith("cli") &&
        !it.name.startsWith("authorization") &&
        !it.name.startsWith("filesystem") &&
        !it.name.startsWith("spark") &&
        !it.name.startsWith("iceberg") &&
        !it.name.startsWith("integration-test") &&
        !it.name.startsWith("flink") &&
        !it.name.startsWith("trino-connector") &&
        it.name != "hive-metastore-common" &&
        it.name != "docs" && it.name != "hadoop-common" && it.parent?.name != "bundles"
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
      ":catalogs:catalog-lakehouse-paimon:copyLibAndConfig",
      "catalogs:catalog-lakehouse-hudi:copyLibAndConfig",
      ":catalogs:catalog-jdbc-doris:copyLibAndConfig",
      ":catalogs:catalog-jdbc-mysql:copyLibAndConfig",
      ":catalogs:catalog-jdbc-oceanbase:copyLibAndConfig",
      ":catalogs:catalog-jdbc-postgresql:copyLibAndConfig",
      ":catalogs:catalog-hadoop:copyLibAndConfig",
      ":catalogs:catalog-kafka:copyLibAndConfig",
      ":catalogs:catalog-model:copyLibAndConfig"
    )
  }

  clean {
    dependsOn(cleanDistribution)
  }
}

apply(plugin = "com.dorongold.task-tree")

project.extra["dockerTest"] = false
project.extra["dockerRunning"] = false
project.extra["macDockerConnector"] = false
project.extra["isOrbStack"] = false

// The following is to check the docker status and print the tip message
fun printDockerCheckInfo() {
  checkMacDockerConnector()
  checkDockerStatus()
  checkOrbStackStatus()

  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode != "deploy" && testMode != "embedded") {
    return
  }
  val dockerRunning = project.extra["dockerRunning"] as? Boolean ?: false
  val macDockerConnector = project.extra["macDockerConnector"] as? Boolean ?: false
  val isOrbStack = project.extra["isOrbStack"] as? Boolean ?: false
  val skipDockerTests = if (extra["skipDockerTests"].toString().toBoolean()) {
    // Read the environment variable (SKIP_DOCKER_TESTS) when skipDockerTests is true
    // which means users can enable the docker tests by setting the gradle properties or the environment variable.
    System.getenv("SKIP_DOCKER_TESTS")?.toBoolean() ?: true
  } else {
    false
  }
  if (skipDockerTests) {
    project.extra["dockerTest"] = false
  } else if (OperatingSystem.current().isMacOsX() &&
    dockerRunning &&
    (macDockerConnector || isOrbStack)
  ) {
    project.extra["dockerTest"] = true
  } else if (OperatingSystem.current().isLinux() && dockerRunning) {
    project.extra["dockerTest"] = true
  }

  println("------------------ Check Docker environment ---------------------")
  println("Docker server status ............................................ [${if (dockerRunning) "running" else "\u001B[31mstop\u001B[0m"}]")
  if (OperatingSystem.current().isMacOsX()) {
    println("mac-docker-connector status ..................................... [${if (macDockerConnector) "running" else "\u001B[31mstop\u001B[0m"}]")
    println("OrbStack status ................................................. [${if (dockerRunning && isOrbStack) "yes" else "\u001B[31mno\u001B[0m"}]")
  }

  val dockerTest = project.extra["dockerTest"] as? Boolean ?: false
  if (dockerTest) {
    println("Using Docker container to run all tests ......................... [$testMode test]")
  } else {
    println("Run test cases without `gravitino-docker-test` tag .............. [$testMode test]")
  }
  println("-----------------------------------------------------------------")

  // Print help message if Docker server or mac-docker-connector is not running
  printDockerServerTip()
  printMacDockerTip()
}

fun printDockerServerTip() {
  val dockerRunning = project.extra["dockerRunning"] as? Boolean ?: false
  if (!dockerRunning) {
    val redColor = "\u001B[31m"
    val resetColor = "\u001B[0m"
    println("Tip: Please make sure to start the ${redColor}Docker server$resetColor before running the integration tests.")
  }
}

fun printMacDockerTip() {
  val macDockerConnector = project.extra["macDockerConnector"] as? Boolean ?: false
  val isOrbStack = project.extra["isOrbStack"] as? Boolean ?: false
  if (OperatingSystem.current().isMacOsX() && !macDockerConnector && !isOrbStack) {
    val redColor = "\u001B[31m"
    val resetColor = "\u001B[0m"
    println(
      "Tip: Please make sure to use ${redColor}OrbStack$resetColor or execute the " +
        "$redColor`dev/docker/tools/mac-docker-connector.sh`$resetColor script before running" +
        " the integration test on macOS."
    )
  }
}

fun checkMacDockerConnector() {
  if (!OperatingSystem.current().isMacOsX()) {
    // Only macOS requires the use of `docker-connector`
    return
  }

  try {
    val processName = "docker-connector"
    val command = "pgrep -x -q $processName"

    val execResult = project.exec {
      commandLine("bash", "-c", command)
    }
    if (execResult.exitValue == 0) {
      project.extra["macDockerConnector"] = true
    }
  } catch (e: Exception) {
    println("checkContainerRunning command execution failed: ${e.message}")
  }
}

fun checkDockerStatus() {
  try {
    val process = ProcessBuilder("docker", "info").start()
    val exitCode = process.waitFor()

    if (exitCode == 0) {
      project.extra["dockerRunning"] = true
    } else {
      println("checkDockerStatus command execution failed with exit code $exitCode")
    }
  } catch (e: IOException) {
    println("checkDockerStatus command execution failed: ${e.message}")
  }
}

fun checkOrbStackStatus() {
  if (!OperatingSystem.current().isMacOsX()) {
    return
  }

  try {
    val process = ProcessBuilder("docker", "context", "show").start()
    val exitCode = process.waitFor()
    if (exitCode == 0) {
      val currentContext = process.inputStream.bufferedReader().readText()
      println("Current docker context is: $currentContext")
      project.extra["isOrbStack"] = currentContext.lowercase().contains("orbstack")
    } else {
      println("checkOrbStackStatus Command execution failed with exit code $exitCode")
    }
  } catch (e: IOException) {
    println("checkOrbStackStatus command execution failed: ${e.message}")
  }
}

printDockerCheckInfo()
