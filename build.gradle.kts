/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach

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

allprojects {
  apply(plugin = "jacoco")

  repositories {
    mavenCentral()
    mavenLocal()
  }

  tasks.configureEach<Test> {
    useJUnitPlatform()
    finalizedBy(tasks.getByName("jacocoTestReport"))
  }

//  tasks.jacocoTestReport {
//    reports {
//      csv.required.set(true)
//      html.required.set(true)
//    }
//  }

//  tasks.jacocoTestReport {
//    reports {
//      csv.apply {
//        isEnabled = true
//      }
//      html.apply {
//        isEnabled = false
//      }
//    }
//  }

//  tasks.withType<JacocoReport> {
//    reports {
//      csv.apply {
//        isEnabled = true
////      destination = File("build/reports/jacoco.xml")
//      }
//      html.apply {
//        isEnabled = false
//      }
//      xml.apply {
//        isEnabled = true
//      }
////      executionData(tasks.withType<Test>())
//    }
//  }

    tasks.withType<JacocoReport> {
    reports {
      csv.required.set(true)
      xml.required.set(false)
      html.required.set(true)
    }
  }

  group = "com.datastrato.graviton"
  version = "${version}"

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

//tasks.withType<JacocoReport> {
//  reports {
//    csv.apply {
//      isEnabled = true
////      destination = File("build/reports/jacoco.xml")
//    }
//    executionData(tasks.withType<Test>())
//  }
//}

//tasks.jacocoTestReport {
//  reports {
//    csv.required.set(true)
//    html.required.set(true)
//  }
//}

//tasks {
//  val jacocoRootReport by registering(JacocoReport::class) {
//    dependsOn(subprojects.map { it.tasks.withType<Test>() })
//    dependsOn(subprojects.map { it.tasks.withType<JacocoReport>() })
//    additionalSourceDirs.setFrom(subprojects.map { it.the<SourceSetContainer>()["main"].allSource.srcDirs })
//    sourceDirectories.setFrom(subprojects.map { it.project.the<SourceSetContainer>()["main"].allSource.srcDirs })
//    classDirectories.setFrom(subprojects.map { it.project.the<SourceSetContainer>()["main"].output })
//    executionData.setFrom(project.fileTree(".") {
//      include("**/build/jacoco/test.exec")
//    })
//    reports {
//      csv.apply {
//        isEnabled = true
////        destination = File("build/reports/jacoco.xml")
//      }
//    }
//  }
//}


//tasks.register("jacocoTestReport", JacocoReport::class) {
//  group = "Reporting"
//  description = "Generate Jacoco coverage reports"
//
//  reports {
//    csv.configure("isEnabled") = false
//    html.isEnabled = false
//    xml.isEnabled = true
//  }
//
//  sourceDirectories.setFrom(files("${project.projectDir}/src/main"))
//  classDirectories.setFrom(
//          fileTree("${buildDir}/intermediates/javac/debug/classes") {
//            setExcludes(setOf("**/BuildConfig.class", "src/main/gen/**/*", "src/main/assets/**/*"))
//          }
//  )
//  executionData.setFrom(
//          fileTree(project.projectDir) {
//            setIncludes(setOf("**/**/*.exec", "**/**/*.ec"))
//          }
//  )
//}
//


//tasks.jacocoTestReport {
//  reports {
//    xml.isEnabled = false
//    csv.isEnabled = false
//    html.isEnabled = true
//    html.destination = file("$buildDir/reports/coverage")
//  }
//  reports {
//    html.isEnabled = true
//    xml.isEnabled = false
//    csv.isEnabled = true
////      각 리포트 타입 마다 리포트 저장 경로를 설정할 수 있다.
////      html.destination = file("$buildDir/jacocoHtml")
////      xml.destination = file("$buildDir/jacoco.xml")
//  }

//  reports {
//    html {
//      isEnabled = true
//    }
//    xml {
//      isEnabled = false
//    }
//    csv {
//      isEnabled = true
//    }
//  }
//}
