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
import org.gradle.api.artifacts.transform.CacheableTransform
import org.gradle.api.artifacts.transform.InputArtifact
import org.gradle.api.artifacts.transform.TransformAction
import org.gradle.api.artifacts.transform.TransformOutputs
import org.gradle.api.artifacts.transform.TransformParameters
import org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE
import org.gradle.api.artifacts.type.ArtifactTypeDefinition.JAR_TYPE
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipOutputStream

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

description = "paimon-rest-service"

// Remove the engine connectors' bundled Zstd for the dedicated upgraded-codec test.
@CacheableTransform
abstract class RemoveBundledZstd : TransformAction<TransformParameters.None> {
  @get:InputArtifact
  @get:PathSensitive(PathSensitivity.NAME_ONLY)
  abstract val inputArtifact: Provider<FileSystemLocation>

  override fun transform(outputs: TransformOutputs) {
    val input = inputArtifact.get().asFile
    if (!input.name.startsWith("paimon-")) {
      outputs.file(input)
      return
    }
    ZipFile(input).use { source ->
      val entries = source.entries().toList()
      if (entries.none { isBundledZstd(it.name) }) {
        outputs.file(input)
        return
      }
      val output = outputs.file(input.name)
      ZipOutputStream(output.outputStream().buffered()).use { target ->
        for (entry in entries) {
          if (isBundledZstd(entry.name)) {
            continue
          }
          target.putNextEntry(ZipEntry(entry.name).apply { time = entry.time })
          source.getInputStream(entry).use { it.copyTo(target) }
          target.closeEntry()
        }
      }
    }
  }

  private fun isBundledZstd(name: String) =
    name.startsWith("com/github/luben/zstd/") ||
      name.substringAfterLast('/').startsWith("libzstd-jni-") ||
      name.startsWith("META-INF/maven/com.github.luben/zstd-jni/")
}

val paimonRuntimeJarType = "paimon-runtime-jar"
dependencies.registerTransform(RemoveBundledZstd::class) {
  from.attribute(ARTIFACT_TYPE_ATTRIBUTE, JAR_TYPE)
  to.attribute(ARTIFACT_TYPE_ATTRIBUTE, paimonRuntimeJarType)
}

fun paimonRuntimeFiles(configuration: Configuration) = configuration.incoming.artifactView {
  attributes.attribute(ARTIFACT_TYPE_ATTRIBUTE, paimonRuntimeJarType)
}.files

val productionRuntimeClasspath = configurations.runtimeClasspath.get()

// The Paimon Spark 3.5 connector is published for Scala 2.12.
val sparkTestScalaVersion = "2.12"
val paimonVersion = libs.versions.paimonRest.get()

// The 1.2 client runs in its own JVM so it cannot resolve Paimon classes from the service.
val paimon12TestClient = sourceSets.create("paimon12TestClient")
val paimon12TestVersion = "1.2.0"

// Keep each engine client isolated from the server and from the other client versions.
val paimonEngineTestConnectors = listOf("1.4.2", "2.0.0", paimonVersion).associateWith { version ->
  configurations.detachedConfiguration(
    dependencies.create("org.apache.paimon:paimon-flink-1.20:$version"),
    dependencies.create("org.apache.paimon:paimon-spark-3.5_$sparkTestScalaVersion:$version")
  ).apply { isTransitive = false }
}

val engineTestRuntime by configurations.creating {
  isCanBeConsumed = false
  attributes.attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
}
configurations.testImplementation { extendsFrom(engineTestRuntime) }

dependencies {
  implementation(project(":api"))
  implementation(project(":clients:client-java"))
  implementation(project(":catalogs:catalog-common"))
  implementation(project(":core")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }
  implementation(project(":server-common")) {
    exclude("*")
  }
  implementation(project(":paimon:paimon-common"))
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.jwt)
  runtimeOnly(libs.nimbus.jose.jwt)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.metrics)
  implementation(libs.bundles.prometheus)
  implementation(libs.caffeine)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.metrics.jersey2)
  runtimeOnly(project(":bundles:aws")) {
    exclude("*")
  }
  runtimeOnly(project(":bundles:aliyun")) {
    exclude("*")
  }
  runtimeOnly(libs.aws.sts)
  runtimeOnly(libs.aws.policy)
  runtimeOnly(libs.aliyun.credentials.sdk) {
    exclude(group = "org.jacoco", module = "org.jacoco.agent")
    // Aliyun's request signer uses JAXB's API. Its legacy implementation activates Jersey WADL.
    exclude(group = "com.sun.xml.bind", module = "jaxb-core")
    exclude(group = "com.sun.xml.bind", module = "jaxb-impl")
  }

  testImplementation(project(":core", "testArtifacts"))
  testCompileOnly("org.apache.paimon:paimon-spark-3.5_$sparkTestScalaVersion:$paimonVersion") {
    isTransitive = false
  }
  testCompileOnly("org.apache.paimon:paimon-flink-1.20:$paimonVersion") {
    isTransitive = false
  }
  val flinkVersion = libs.versions.flink120.get()
  engineTestRuntime("org.apache.flink:flink-streaming-java:$flinkVersion")
  engineTestRuntime("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
  engineTestRuntime("org.apache.flink:flink-table-planner-loader:$flinkVersion")
  engineTestRuntime("org.apache.flink:flink-table-runtime:$flinkVersion")
  engineTestRuntime("org.apache.flink:flink-clients:$flinkVersion")
  engineTestRuntime("org.apache.flink:flink-connector-base:$flinkVersion")
  engineTestRuntime("org.apache.flink:flink-connector-files:$flinkVersion")
  engineTestRuntime("org.apache.spark:spark-sql_$sparkTestScalaVersion:${libs.versions.spark35.get()}") {
    exclude("org.apache.hadoop")
  }
  // Paimon's Spark extensions reference Spark's Hive expression classes in REST sessions as well.
  engineTestRuntime("org.apache.spark:spark-hive_$sparkTestScalaVersion:${libs.versions.spark35.get()}") {
    exclude("org.apache.hadoop")
  }
  engineTestRuntime(libs.hadoop3.client.api)
  engineTestRuntime(libs.hadoop3.client.runtime)
  engineTestRuntime(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.aws.sts)
  testImplementation(libs.aliyun.credentials.sdk)
  testImplementation(libs.sqlite.jdbc)
  testRuntimeOnly(libs.junit.jupiter.engine)

  add(paimon12TestClient.implementationConfigurationName, "org.apache.paimon:paimon-core:$paimon12TestVersion")
  add(paimon12TestClient.runtimeOnlyConfigurationName, "org.apache.paimon:paimon-format:$paimon12TestVersion")
  add(paimon12TestClient.runtimeOnlyConfigurationName, libs.hadoop3.client.api)
  add(paimon12TestClient.runtimeOnlyConfigurationName, libs.hadoop3.client.runtime)
  add(paimon12TestClient.runtimeOnlyConfigurationName, libs.commons.lang3)
  add(paimon12TestClient.runtimeOnlyConfigurationName, libs.slf4j.simple)
}

// Paimon uses the at.yawk.lz4 fork of lz4-java which declares the same Gradle capability
// as org.lz4:lz4-java pulled in by other modules; pick the highest version to resolve it.
configurations.configureEach {
  resolutionStrategy.capabilitiesResolution.withCapability("org.lz4:lz4-java") {
    selectHighestVersion()
  }
}

tasks {
  register("copyLibs", Sync::class) {
    dependsOn("build")
    // Copy only the resolved production dependencies, not stale jars from previous builds.
    from(productionRuntimeClasspath)
    from(jar)
    into("$rootDir/distribution/package/paimon-rest-server/libs")
  }

  register("copyConfigs", Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/paimon-rest-server/conf")

    include("core-site.xml.template")
    include("hdfs-site.xml.template")

    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }

    fileMode = 0b111101101
  }

  register("copyLibAndConfigs", Copy::class) {
    dependsOn("copyLibs", "copyConfigs")
  }

  register("copyLibAndConfigsToStandalonePackage", Sync::class) {
    from(listOf(productionRuntimeClasspath, jar)) {
      into("libs")
    }
    from("src/main/resources") {
      include("core-site.xml.template", "hdfs-site.xml.template")
      rename { it.removeSuffix(".template") }
      into("conf")
    }
    into(layout.buildDirectory.dir("standalone"))
  }
}

tasks.test {
  inputs.files(productionRuntimeClasspath).withPropertyName("paimonProductionRuntimeClasspath")
  systemProperty("paimon.zstd-jni.version", libs.versions.zstd.jni.get())
  doFirst {
    systemProperty("paimon.runtime-classpath", productionRuntimeClasspath.asPath)
  }
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar, tasks.named(paimon12TestClient.classesTaskName))
    val clientClasspath = paimon12TestClient.runtimeClasspath
    inputs.files(clientClasspath).withPropertyName("paimon12TestClientClasspath")
    val engineClasspath = sourceSets.test.get().output + engineTestRuntime
    val patchedEngineConnectors = paimonRuntimeFiles(paimonEngineTestConnectors.getValue(paimonVersion))
    val patchedEngineClasspath = engineClasspath.filter { !it.name.startsWith("zstd-jni-") } +
      productionRuntimeClasspath.filter { it.name.startsWith("zstd-jni-") }
    inputs.files(engineClasspath).withPropertyName("paimonEngineRuntimeClasspath")
    inputs.files(patchedEngineConnectors, patchedEngineClasspath).withPropertyName("paimonPatchedEngineClasspath")
    paimonEngineTestConnectors.forEach { (version, connectors) ->
      inputs.files(connectors).withPropertyName("paimonEngineTestConnectors-$version")
    }
    systemProperty("paimon.test.engine.version", paimonVersion)
    doFirst {
      systemProperty("paimon.test.client12.classpath", clientClasspath.asPath)
      val engineClasspathDirectory = temporaryDir.resolve("engine-classpaths").apply { mkdirs() }
      systemProperty("paimon.test.engine.classpath-directory", engineClasspathDirectory.absolutePath)
      for (engine in listOf("flink", "spark")) {
        val patchedConnector = patchedEngineConnectors.filter { it.name.startsWith("paimon-$engine-") }
        engineClasspathDirectory.resolve("patched-$engine").writeText((patchedConnector + patchedEngineClasspath).asPath)
        paimonEngineTestConnectors.forEach { (version, connectors) ->
          val officialConnector = connectors.filter { it.name.startsWith("paimon-$engine-") }
          engineClasspathDirectory.resolve("$version-$engine").writeText((officialConnector + engineClasspath).asPath)
        }
      }
    }
  }
}
