import com.diffplug.gradle.spotless.SpotlessExtension
import net.ltgt.gradle.errorprone.errorprone

plugins {
  `java-library`
  `maven-publish`
}

val connectorRange = "470-478"
val trinoVersion = "478"
val propagateVersion = providers
  .gradleProperty("trinoConnectorPropagateVersion")
  .map(String::toBoolean)
  .getOrElse(true)

if (propagateVersion) {
  gradle.beforeProject {
    if (path == ":trino-connector:trino-connector") {
      extensions.extraProperties["trinoConnectorTrinoVersion"] = trinoVersion
    }
  }
}

java {
  toolchain.languageVersion.set(JavaLanguageVersion.of(24))
}

dependencies {
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(libs.airlift.json)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  implementation("io.trino:trino-jdbc:$trinoVersion")
  compileOnly(libs.airlift.resolver)
  compileOnly("io.trino:trino-spi:$trinoVersion") {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(libs.awaitility)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mysql.driver)
  testImplementation("io.trino:trino-memory:$trinoVersion") {
    exclude("org.antlr")
    exclude("org.apache.logging.log4j")
  }
  testImplementation("io.trino:trino-testing:$trinoVersion") {
    exclude("org.apache.logging.log4j")
  }
  testRuntimeOnly(libs.junit.jupiter.engine)
}

sourceSets {
  main {
    java.srcDirs("../trino-connector/src/main/java")
    resources.srcDirs("../trino-connector/src/main/resources")
  }
}

plugins.withId("com.diffplug.spotless") {
  configure<SpotlessExtension> {
    java {
      // Keep Spotless within this module to avoid cross-project target errors.
      target("$projectDir/src/**/*.java")
    }
  }
}

tasks.withType<JavaCompile>().configureEach {
  // Error Prone is incompatible with the JDK 24 toolchain required by this Trino range.
  options.errorprone.isEnabled.set(false)
}

tasks {
  val copyRuntimeLibs by registering(Copy::class) {
    dependsOn("jar")
    from({ configurations.runtimeClasspath.get().filter(File::isFile) })
    into(layout.buildDirectory.dir("libs"))
  }

  named("build") {
    finalizedBy(copyRuntimeLibs)
  }
}
