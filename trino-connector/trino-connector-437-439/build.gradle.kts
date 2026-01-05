import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.Copy
import org.gradle.jvm.tasks.Jar

plugins {
  `java-library`
  `maven-publish`
}

val connectorRange = "437-439"
val trinoVersion = "437"
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
  toolchain.languageVersion.set(JavaLanguageVersion.of(17))
}

dependencies {
  implementation(project(":trino-connector:trino-connector"))
  compileOnly("io.trino:trino-spi:$trinoVersion") {
    exclude("org.apache.logging.log4j")
  }
}

val commonJarTask = project(":trino-connector:trino-connector").tasks.named<Jar>("jar")
val commonJarFile = commonJarTask.flatMap { it.archiveFile }
val copyDependsTask = project(":trino-connector:trino-connector").tasks.named<Copy>("copyDepends")
val copyRuntimeDeps by tasks.registering(Copy::class) {
  dependsOn(configurations.runtimeClasspath)
  dependsOn(copyDependsTask)
  from({
    configurations.runtimeClasspath.get().filter(File::isFile)
  })
  into(layout.buildDirectory.dir("libs"))
}

tasks.withType<Test>().configureEach {
  enabled = false
}

tasks.named<Jar>("jar") {
  dependsOn(commonJarTask)
  dependsOn(copyDependsTask)
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
  archiveBaseName.set("gravitino-trino-connector-$connectorRange")
  from(commonJarFile.map { zipTree(it.asFile) }) {
    exclude("META-INF/MANIFEST.MF")
  }
  manifest {
    attributes(
      "Implementation-Title" to "Gravitino Trino Connector $connectorRange",
      "Trino-Version" to trinoVersion
    )
  }
}

tasks.named("generateMetadataFileForMavenJavaPublication") {
  dependsOn(copyDependsTask)
}

tasks.named("assemble") {
  dependsOn(copyRuntimeDeps)
}
