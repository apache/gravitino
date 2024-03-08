/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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

  if (project.name != "meta") {
    apply(plugin = "net.ltgt.errorprone")
    dependencies {
      errorprone("com.google.errorprone:error_prone_core:2.10.0")
    }

    tasks.withType<JavaCompile>().configureEach {
      options.errorprone.isEnabled.set(true)
      options.errorprone.disableAllChecks.set(true)
      options.errorprone.enable(
        "AnnotateFormatMethod",
        "FormatStringAnnotation",
        "AlwaysThrows",
        "ArrayEquals",
        "ArrayToString",
        "ArraysAsListPrimitiveArray",
        "ArrayFillIncompatibleType",
        "BoxedPrimitiveEquality",
        "ChainingConstructorIgnoresParameter",
        "CheckNotNullMultipleTimes",
        "CollectionIncompatibleType",
        "CollectionToArraySafeParameter",
        "ComparingThisWithNull",
        "ComparisonOutOfRange",
        "CompatibleWithAnnotationMisuse",
        "CompileTimeConstant",
        "ConditionalExpressionNumericPromotion",
        "DangerousLiteralNull",
        "DeadException",
        "DeadThread",
        "DoNotCall",
        "DoNotMock",
        "DuplicateMapKeys",
        "EqualsNaN",
        "EqualsNull",
        "EqualsReference",
        "EqualsWrongThing",
        "ForOverride",
        "FormatString",
        "GetClassOnAnnotation",
        "GetClassOnClass",
        "HashtableContains",
        "IdentityBinaryExpression",
        "IdentityHashMapBoxing",
        "Immutable",
        "Incomparable",
        "IncompatibleArgumentType",
        "IndexOfChar",
        "InfiniteRecursion",
        "InvalidJavaTimeConstant",
        "InvalidPatternSyntax",
        "IsInstanceIncompatibleType",
        "JavaUtilDate",
        "JUnit4ClassAnnotationNonStatic",
        "JUnit4SetUpNotRun",
        "JUnit4TearDownNotRun",
        "JUnit4TestNotRun",
        "JUnitAssertSameCheck",
        "LockOnBoxedPrimitive",
        "LoopConditionChecker",
        "LossyPrimitiveCompare",
        "MathRoundIntLong",
        "MissingSuperCall",
        "ModifyingCollectionWithItself",
        "NonCanonicalStaticImport",
        "NonFinalCompileTimeConstant",
        "NonRuntimeAnnotation",
        "NullTernary",
        "OptionalEquality",
        "PackageInfo",
        "ParametersButNotParameterized",
        "RandomCast",
        "RandomModInteger",
        "SelfAssignment",
        "SelfComparison",
        "SelfEquals",
        "SizeGreaterThanOrEqualsZero",
        "StreamToString",
        "StringBuilderInitWithChar",
        "SubstringOfZero",
        "ThrowNull",
        "TruthSelfEquals",
        "TryFailThrowable",
        "TypeParameterQualifier",
        "UnnecessaryCheckNotNull",
        "UnnecessaryTypeArgument",
        "UnusedAnonymousClass",
        "UnusedCollectionModifiedInPlace",
        "UseCorrectAssertInTests",
        "VarTypeName",
        "XorPower",
        "EqualsGetClass",
        "DefaultCharset"
      )
    }
  }

  tasks.withType<Javadoc> {
    options.encoding = "UTF-8"
    options.locale = "en_US"

    val projectName = project.name
    if (projectName == "common" || projectName == "api" || projectName == "client-java") {
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

  if (project.name in listOf("web", "docs")) {
    plugins.apply(NodePlugin::class)
    configure<NodeExtension> {
      version.set("20.9.0")
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

      jvmArgs(project.property("extraJvmArgs") as List<*>)
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
  substringMatcher("DS", "Datastrato", "Copyright 2023 Datastrato Pvt Ltd.")
  substringMatcher("DS", "Datastrato", "Copyright 2024 Datastrato Pvt Ltd.")
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
    "web/next-env.d.ts",
    "web/dist/**/*",
    "web/node_modules/**/*",
    "web/lib/utils/axios/**/*",
    "web/lib/enums/httpEnum.ts",
    "web/types/axios.d.ts",
    "web/yarn.lock",
    "web/package-lock.json",
    "web/pnpm-lock.yaml",
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
        it.name != "integration-test" && it.name != "bundled-catalog" && it.name != "spark-connector"
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
        it.name != "spark-connector" &&
        it.name != "integration-test" &&
        it.name != "bundled-catalog"
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
      ":catalogs:catalog-jdbc-postgresql:copyLibAndConfig",
      ":catalogs:catalog-hadoop:copyLibAndConfig"
    )
  }

  clean {
    dependsOn(cleanDistribution)
  }
}

apply(plugin = "com.dorongold.task-tree")
