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
description = "catalog-hadoop"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api")) {
    exclude(group = "*")
  }

  implementation(project(":core")) {
    exclude(group = "*")
  }
  implementation(project(":common")) {
    exclude(group = "*")
  }

  compileOnly(libs.guava)

  implementation(libs.hadoop3.common) {
    exclude("com.sun.jersey")
    exclude("javax.servlet", "servlet-api")
    exclude("org.eclipse.jetty", "*")
    exclude("org.apache.hadoop", "hadoop-auth")
    exclude("org.apache.curator", "curator-client")
    exclude("org.apache.curator", "curator-framework")
    exclude("org.apache.curator", "curator-recipes")
    exclude("org.apache.avro", "avro")
    exclude("com.sun.jersey", "jersey-servlet")
  }

  implementation(libs.hadoop3.hdfs) {
    exclude("com.sun.jersey")
    exclude("javax.servlet", "servlet-api")
    exclude("com.google.guava", "guava")
    exclude("commons-io", "commons-io")
    exclude("org.eclipse.jetty", "*")
    exclude("io.netty")
    exclude("org.fusesource.leveldbjni")
  }
  implementation(libs.hadoop3.client) {
    exclude("org.apache.hadoop", "hadoop-mapreduce-client-core")
    exclude("org.apache.hadoop", "hadoop-mapreduce-client-jobclient")
    exclude("org.apache.hadoop", "hadoop-yarn-api")
    exclude("org.apache.hadoop", "hadoop-yarn-client")
    exclude("com.squareup.okhttp", "okhttp")
  }

  implementation(libs.slf4j.api)

  testImplementation(project(":clients:client-java"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server"))
  testImplementation(project(":server-common"))
  testImplementation(project(":bundles:aws-bundle"))
  testImplementation(project(":bundles:gcp-bundle"))
  testImplementation(project(":bundles:aliyun-bundle"))

  testImplementation(libs.minikdc)
  testImplementation(libs.hadoop3.minicluster)

  testImplementation(libs.bundles.log4j)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.mysql)
  testImplementation(libs.hadoop3.gcs)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }

  val copyCatalogLibs by registering(Copy::class) {
    dependsOn("jar", "runtimeJars")
    from("build/libs") {
      exclude("slf4j-*.jar")
      exclude("guava-*.jar")
      exclude("curator-*.jar")
      exclude("netty-*.jar")
      exclude("snappy-*.jar")
      exclude("zookeeper-*.jar")
      exclude("jetty-*.jar")
      exclude("javax.servlet-*.jar")
      exclude("kerb-*.jar")
      exclude("kerby-*.jar")
    }
    into("$rootDir/distribution/package/catalogs/hadoop/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/hadoop/conf")

    include("hadoop.conf")
    include("core-site.xml.template")
    include("hdfs-site.xml.template")

    rename { original ->
      if (original.endsWith(".template")) {
        original.replace(".template", "")
      } else {
        original
      }
    }

    exclude { details ->
      details.file.isDirectory()
    }

    fileMode = 0b111101101
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyCatalogConfig, copyCatalogLibs)
  }
}

tasks.test {
  doFirst {
    val testMode = project.properties["testMode"] as? String ?: "embedded"
    if (testMode == "deploy") {
      environment("GRAVITINO_HOME", project.rootDir.path + "/distribution/package")
    } else if (testMode == "embedded") {
      environment("GRAVITINO_HOME", project.rootDir.path)
    }
  }

  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }

  // this task depends on :bundles:aws-bundle:jar
  dependsOn(":bundles:aws-bundle:jar")
  dependsOn(":bundles:aliyun-bundle:jar")
  dependsOn(":bundles:gcp-bundle:jar")
}

tasks.getByName("generateMetadataFileForMavenJavaPublication") {
  dependsOn("runtimeJars")
}
