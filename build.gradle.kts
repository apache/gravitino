import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach

plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.gradle.extensions)
  alias(libs.plugins.spotless)
  alias(libs.plugins.publish)
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

  tasks.configureEach<Test> {
    useJUnitPlatform()
  }

  group = "com.datastrato.unified_catalog"
  version = "${version}"

  plugins.withType<SpotlessPlugin>().configureEach {
    configure<SpotlessExtension> {
      java {
        googleJavaFormat()
        removeUnusedImports()
        trimTrailingWhitespace()
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
