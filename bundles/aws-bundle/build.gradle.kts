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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipOutputStream

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

/**
 * Removes the unrelocated Netty copy that aws-java-sdk-bundle (AWS SDK for Java v1, pulled in by
 * hadoop-aws) embeds under io/netty.
 *
 * A dependency constraint cannot replace classes that live inside another jar. Left in place, those
 * classes are merged with the standalone Netty jars used by the AWS SDK v2, and the bundle ends up
 * with classes and metadata of two different Netty releases. The standalone Netty jars provide every
 * module embedded in aws-java-sdk-bundle, and the embedded classes are binary compatible with them
 * within the 4.1 line, so removing the embedded copy leaves a single Netty release in the bundle.
 * Other jars pass through unchanged.
 */
@CacheableTransform
abstract class StripEmbeddedNettyTransform : TransformAction<TransformParameters.None> {
  companion object {
    val STRIPPED: Attribute<Boolean> =
      Attribute.of("org.apache.gravitino.embedded-netty-stripped", Boolean::class.javaObjectType)
    const val EMBEDDING_JAR_PREFIX = "aws-java-sdk-bundle-"
    private val NETTY_ENTRY_PREFIXES = listOf(
      "io/netty/",
      "META-INF/maven/io.netty/",
      "META-INF/native-image/io.netty/"
    )
    private val NETTY_ENTRIES = setOf(
      "META-INF/io.netty.versions.properties",
      // Only lists Netty's BlockHound integration, which the standalone netty-common also provides.
      "META-INF/services/reactor.blockhound.integration.BlockHoundIntegration"
    )

    /** Returns true if the jar entry [name] belongs to the embedded Netty copy. */
    fun isNettyEntry(name: String): Boolean =
      name in NETTY_ENTRIES || NETTY_ENTRY_PREFIXES.any { name.startsWith(it) }
  }

  @get:InputArtifact
  @get:PathSensitive(PathSensitivity.NAME_ONLY)
  abstract val inputArtifact: Provider<FileSystemLocation>

  override fun transform(outputs: TransformOutputs) {
    val input = inputArtifact.get().asFile
    if (!input.isFile || !input.name.startsWith(EMBEDDING_JAR_PREFIX) || !input.name.endsWith(".jar")) {
      outputs.file(inputArtifact)
      return
    }
    val hasEmbeddedNetty = ZipFile(input).use { jar -> jar.entries().asSequence().any { isNettyEntry(it.name) } }
    if (!hasEmbeddedNetty) {
      outputs.file(inputArtifact)
      return
    }
    // Keep the original file name so that the bundled legal files are still resolved by it.
    val output = outputs.file(input.name)
    ZipFile(input).use { jar ->
      ZipOutputStream(output.outputStream().buffered()).use { out ->
        jar.entries().asSequence().filterNot { isNettyEntry(it.name) }.forEach { source ->
          val entry = ZipEntry(source.name)
          entry.time = source.time
          out.putNextEntry(entry)
          if (!source.isDirectory) {
            jar.getInputStream(source).use { it.copyTo(out) }
          }
          out.closeEntry()
        }
      }
    }
  }
}

// Moves every Netty artifact brought in by the AWS SDK to the single version of the catalog. The
// BOM sits in a dedicated configuration so that it only applies to this module.
val nettyBom: Configuration by configurations.creating {
  isCanBeConsumed = false
  isCanBeResolved = false
}

configurations.named("runtimeClasspath") {
  extendsFrom(nettyBom)
  attributes.attribute(StripEmbeddedNettyTransform.STRIPPED, true)
}

dependencies {
  attributesSchema {
    attribute(StripEmbeddedNettyTransform.STRIPPED)
  }
  artifactTypes.getByName(ArtifactTypeDefinition.JAR_TYPE) {
    attributes.attribute(StripEmbeddedNettyTransform.STRIPPED, false)
  }
  registerTransform(StripEmbeddedNettyTransform::class) {
    from.attribute(StripEmbeddedNettyTransform.STRIPPED, false)
    to.attribute(StripEmbeddedNettyTransform.STRIPPED, true)
  }
  nettyBom(platform(libs.netty.bom))
  implementation(project(":bundles:aws"))
  implementation(libs.hadoop3.aws)
  implementation(libs.aws.iam)
  implementation(libs.aws.policy)
  implementation(libs.aws.sts)
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.slf4j.api)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  dependencies {
    exclude(dependency("org.slf4j:slf4j-api"))
    // The optional OpenSSL provider contains LGPL code. Hadoop's default mode falls back to JSSE.
    exclude(dependency("org.wildfly.openssl:wildfly-openssl"))

    // Exclude Gravitino modules to prevent class duplication and "Split Packages" issues.
    // These modules (api, common, catalogs) are already provided by the Gravitino server and gravitino-filesystem-hadoop3-runtime.
    // Including them here would cause the Relocation rules below to incorrectly modify
    // method signatures (e.g., JsonUtils.anyFieldMapper returning a shaded ObjectMapper),
    // leading to java.lang.NoSuchMethodError at runtime.
    exclude(project(":api"))
    exclude(project(":common"))
    exclude(project(":catalogs:catalog-common"))
    exclude(project(":catalogs:hadoop-common"))
  }

  relocate("com.fasterxml.jackson", "org.apache.gravitino.aws.shaded.com.fasterxml.jackson")
  relocate("com.google.common", "org.apache.gravitino.aws.shaded.com.google.common")
  relocate("com.google.errorprone", "org.apache.gravitino.aws.shaded.com.google.errorprone")
  relocate("com.google.thirdparty", "org.apache.gravitino.aws.shaded.com.google.thirdparty")
  relocate("io.netty", "org.apache.gravitino.aws.shaded.io.netty")
  relocate("org.apache.commons", "org.apache.gravitino.aws.shaded.org.apache.commons")
  relocate("org.apache.http", "org.apache.gravitino.aws.shaded.org.apache.http")
  relocate("org.checkerframework", "org.apache.gravitino.aws.shaded.org.checkerframework")
  relocate("org.reactivestreams", "org.apache.gravitino.aws.shaded.org.reactivestreams")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-fileset:runtimeJars")
}

tasks.test {
  val bundle = tasks.named<ShadowJar>("shadowJar")
  dependsOn(bundle)
  inputs.file(bundle.flatMap { it.archiveFile })
  doFirst { systemProperty("shadowJarPath", bundle.get().archiveFile.get().asFile.absolutePath) }
  systemProperty("nettyVersion", libs.versions.netty.get())
}
