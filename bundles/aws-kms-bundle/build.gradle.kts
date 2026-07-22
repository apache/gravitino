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
  implementation(project(":bundles:aws")) {
    isTransitive = false
  }
  implementation(libs.aws.kms) {
    // KMS inspection uses only the synchronous client.
    exclude(group = "software.amazon.awssdk", module = "netty-nio-client")
  }
  // Required by the AWS default credential chain for web-identity credentials.
  implementation(libs.aws.sts) {
    exclude(group = "software.amazon.awssdk", module = "netty-nio-client")
  }
  implementation(libs.guava)
}

tasks.withType(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveClassifier.set("")

  dependencies {
    exclude(dependency("org.slf4j:slf4j-api"))

    // Gravitino interfaces and utilities are supplied by the server.
    exclude(project(":api"))
    exclude(project(":common"))
    exclude(project(":catalogs:catalog-common"))
    exclude(project(":catalogs:hadoop-common"))
  }

  // The source module also contains S3 filesystem and credential-vending implementations.
  exclude("org/apache/gravitino/s3/**")
  exclude("META-INF/services/org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider")
  exclude("META-INF/services/org.apache.gravitino.credential.CredentialProvider")
  exclude("META-INF/services/org.apache.gravitino.s3.credential.webidentity.WebIdentityTokenSource")

  relocate("com.fasterxml.jackson", "org.apache.gravitino.kms.aws.shaded.com.fasterxml.jackson")
  relocate("com.google.common", "org.apache.gravitino.kms.aws.shaded.com.google.common")
  relocate("com.google.errorprone", "org.apache.gravitino.kms.aws.shaded.com.google.errorprone")
  relocate("com.google.thirdparty", "org.apache.gravitino.kms.aws.shaded.com.google.thirdparty")
  relocate("io.netty", "org.apache.gravitino.kms.aws.shaded.io.netty")
  relocate("org.apache.commons", "org.apache.gravitino.kms.aws.shaded.org.apache.commons")
  relocate("org.apache.http", "org.apache.gravitino.kms.aws.shaded.org.apache.http")
  relocate("org.checkerframework", "org.apache.gravitino.kms.aws.shaded.org.checkerframework")
  relocate("org.reactivestreams", "org.apache.gravitino.kms.aws.shaded.org.reactivestreams")
  relocate("org.wildfly.openssl", "org.apache.gravitino.kms.aws.shaded.org.wildfly.openssl")

  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
