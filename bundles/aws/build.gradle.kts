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

plugins {
  `maven-publish`
  id("java")
}

dependencies {
  implementation(project(":api")) {
    exclude("*")
  }
  implementation(project(":catalogs:catalog-common")) {
    exclude("*")
  }
  implementation(project(":catalogs:hadoop-common")) {
    exclude("*")
  }
  implementation(project(":common")) {
    exclude("*")
  }

  implementation(libs.commons.lang3)
  implementation(libs.guava)

  compileOnly(libs.aws.iam)
  compileOnly(libs.aws.policy)
  compileOnly(libs.aws.sts)
  compileOnly(libs.hadoop3.aws) {
    exclude("software.amazon.awssdk", "bundle")
  }
  compileOnly(libs.hadoop3.client.api)
  // AWS SDK v1 is only needed to compile S3CredentialsProvider, which serves Hadoop 3.3 and earlier.
  // The thin bundle does not ship it, the Hadoop runtime provides it.
  compileOnly(libs.aws.v1.core) {
    isTransitive = false
  }

  testCompileOnly(libs.aws.v1.core) {
    isTransitive = false
  }
  testImplementation(libs.aws.iam)
  testImplementation(libs.aws.policy)
  testImplementation(libs.aws.sts)
  testImplementation(libs.hadoop3.aws) {
    exclude("software.amazon.awssdk", "bundle")
  }
  testImplementation(libs.bundles.hadoop.aws.sdk)
  testImplementation(libs.hadoop3.client.api)
  testImplementation(libs.hadoop3.client.runtime)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.compileJava {
  dependsOn(":catalogs:catalog-fileset:runtimeJars")
}

// The thin bundle runs on the Hadoop of its host, which can be 3.3 (AWS SDK v1, for example Spark 3.5)
// or 3.4 and later (AWS SDK v2). The default test task runs against the Hadoop version of the
// project. The tasks below run the AWS SDK v1 tests against the oldest supported Hadoop 3.3, once
// with the AWS SDK v1 only and once with the AWS SDK v2 also on the classpath, as in a Spark 3.5
// job that uses the Iceberg AWS bundle.
val hadoop33Version: String = libs.versions.hadoop33.get()

fun hadoop33TestRuntime(withSdkV2: Boolean): Configuration =
  configurations.testRuntimeClasspath.get().copyRecursive().apply {
    resolutionStrategy.eachDependency {
      if (requested.group == "org.apache.hadoop") {
        useVersion(hadoop33Version)
      }
    }
    if (!withSdkV2) {
      exclude(group = "software.amazon.awssdk")
    }
  }

fun registerHadoop33Test(name: String, withSdkV2: Boolean): TaskProvider<Test> {
  val runtime = hadoop33TestRuntime(withSdkV2)
  return tasks.register<Test>(name) {
    group = "verification"
    description =
      "Runs the AWS SDK v1 tests against Hadoop $hadoop33Version" +
      if (withSdkV2) " with the AWS SDK v2 on the classpath" else ""
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().output + sourceSets.main.get().output + runtime
    include("**/*V1.class", "**/TestS3CredentialsProviderSelection.class")
    systemProperty("s3a.expectedSdk", "v1")
    systemProperty("s3a.sdkV1OnClasspath", "true")
    systemProperty("s3a.sdkV2OnClasspath", withSdkV2.toString())
    onlyIf { !project.hasProperty("skipTests") }
  }
}

val testHadoop33 = registerHadoop33Test("testHadoop33", withSdkV2 = false)
val testHadoop33WithSdkV2 = registerHadoop33Test("testHadoop33WithSdkV2", withSdkV2 = true)

tasks.test {
  // The AWS SDK v1 tests run in the Hadoop 3.3 tasks, the AWS SDK v1 is not on this classpath.
  exclude("**/*V1.class")
  val hadoop33 = libs.versions.hadoop3.asProvider().get().startsWith("3.3.")
  systemProperty("s3a.expectedSdk", if (hadoop33) "v1" else "v2")
  systemProperty("s3a.sdkV1OnClasspath", hadoop33.toString())
  systemProperty("s3a.sdkV2OnClasspath", "true")
  // Run them with `./gradlew test` too, not only with `check`.
  finalizedBy(testHadoop33, testHadoop33WithSdkV2)
}

tasks.check {
  dependsOn(testHadoop33, testHadoop33WithSdkV2)
}
