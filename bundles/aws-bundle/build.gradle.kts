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
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":core"))
  compileOnly(project(":catalogs:catalog-common"))
  compileOnly(project(":catalogs:catalog-hadoop"))
  compileOnly(libs.hadoop3.common)

  implementation(libs.aws.iam)
  implementation(libs.aws.policy)
  implementation(libs.aws.sts)
  compileOnly(libs.hadoop3.aws)
  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }
}

tasks.register<Copy>("copyHadoop2_7") {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")

  val commonsLang3Dependency = libs.commons.lang3.get()
  val hadoopAWSDependencies = libs.hadoop207.aws.get()

  val hadoopDependenciesFor2_7 = configurations.detachedConfiguration(
    dependencies.create("${hadoopAWSDependencies.group}:${hadoopAWSDependencies.name}:${hadoopAWSDependencies.version}"),
    dependencies.create("${commonsLang3Dependency.group}:${commonsLang3Dependency.name}:${commonsLang3Dependency.version}")
  ).files

  from(hadoopDependenciesFor2_7)
  into("$buildDir/resources/main/hadoop2_7")

  rename { original ->
    if (original.endsWith(".jar")) {
      original.replace(".jar", ".jar.zip")
    } else {
      original
    }
  }
}

tasks.register<Copy>("copyHadoop2_10") {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")

  val commonsLangDependency = libs.commons.lang.get()
  val commonsLang3Dependency = libs.commons.lang3.get()
  val hadoopAWSDependencies = libs.hadoop210.aws.get()

  val hadoopDependenciesFor2_10 = configurations.detachedConfiguration(
    dependencies.create("${hadoopAWSDependencies.group}:${hadoopAWSDependencies.name}:${hadoopAWSDependencies.version}"),
    dependencies.create("${commonsLang3Dependency.group}:${commonsLang3Dependency.name}:${commonsLang3Dependency.version}"),
    dependencies.create("${commonsLangDependency.group}:${commonsLangDependency.name}:${commonsLangDependency.version}")
  ).files

  from(hadoopDependenciesFor2_10)
  into("$buildDir/resources/main/hadoop2_10")

  // We need to rename the jars to zip to avoid because gradle can't distinguish resources from dependencies if the
  // extension is the same.
  rename { original ->
    if (original.endsWith(".jar")) {
      original.replace(".jar", ".jar.zip")
    } else {
      original
    }
  }
}

tasks.register<Copy>("copyHadoop3_3") {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
  val commonsLang3Dependency = libs.commons.lang3.get()
  val hadoopAWSDependencies = libs.hadoop3.aws.get()

  val hadoopDependenciesFor3_3 = configurations.detachedConfiguration(
    dependencies.create("${hadoopAWSDependencies.group}:${hadoopAWSDependencies.name}:${hadoopAWSDependencies.version}"),
    dependencies.create("${commonsLang3Dependency.group}:${commonsLang3Dependency.name}:${commonsLang3Dependency.version}")
  ).files

  from(hadoopDependenciesFor3_3)
  into("$buildDir/resources/main/hadoop3_3")

  rename { original ->
    if (original.endsWith(".jar")) {
      original.replace(".jar", ".jar.zip")
    } else {
      original
    }
  }
}

tasks.withType(ShadowJar::class.java) {
  dependsOn("copyHadoop3_3", "copyHadoop2_7", "copyHadoop2_10")
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-hadoop:runtimeJars")
}

// This is to fix the problem:
/**
Reason: Task ':bundles:aws-bundle:javadoc' uses this output of task ':bundles:aws-bundle:copyHadoop2_10' without declaring an explicit or implicit dependency. This can lead to incorrect results being produced, depending on what order the tasks are executed.

Possible solutions:
1. Declare task ':bundles:aws-bundle:copyHadoop2_10' as an input of ':bundles:aws-bundle:javadoc'.
2. Declare an explicit dependency on ':bundles:aws-bundle:copyHadoop2_10' from ':bundles:aws-bundle:javadoc' using Task#dependsOn.
3. Declare an explicit dependency on ':bundles:aws-bundle:copyHadoop2_10' from ':bundles:aws-bundle:javadoc' using Task#mustRunAfter
*/
tasks.javadoc {
  dependsOn("copyHadoop3_3", "copyHadoop2_7", "copyHadoop2_10")
}
