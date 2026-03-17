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

import org.gradle.api.publish.maven.tasks.PublishToMavenLocal
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository

plugins {
  id("java")
  id("idea")
}

dependencies {
  implementation(libs.hadoop2.common) {
    exclude(group = "org.slf4j")
    exclude(group = "org.apache.logging.log4j")
    exclude(group = "com.google.code.findbugs")
    exclude(group = "org.eclipse.jetty.aggregate", module = "jetty-all")
    exclude(group = "org.eclipse.jetty.orbit", module = "javax.servlet")
  }
  implementation(libs.hive2.metastore) {
    exclude(group = "ant")
    exclude(group = "co.cask.tephra")
    exclude(group = "com.github.joshelser")
    exclude(group = "com.google.code.findbugs")
    exclude(group = "com.tdunning", module = "json")
    exclude(group = "com.zaxxer", module = "HikariCP")
    exclude(group = "io.dropwizard.metrics")
    exclude(group = "javax.transaction", module = "transaction-api")
    exclude(group = "junit")
    exclude(group = "org.apache.ant")
    exclude(group = "org.apache.avro")
    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-server-resourcemanager")
    exclude(group = "org.apache.hbase")
    exclude(group = "org.apache.logging.log4j")
    exclude(group = "org.apache.parquet", module = "parquet-hadoop-bundle")
    exclude(group = "org.datanucleus")
    exclude(group = "org.eclipse.jetty.aggregate", module = "jetty-all")
    exclude(group = "org.eclipse.jetty.orbit", module = "javax.servlet")
    exclude(group = "org.openjdk.jol")
    exclude(group = "org.slf4j")
  }
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }
  jar {
    finalizedBy(copyDepends)
  }

  register("copyLibs", Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs") {
      exclude("guava-*.jar")
      exclude("log4j-*.jar")
      exclude("slf4j-*.jar")
      exclude("logback-*.jar")
      exclude("error_prone_annotations-*.jar")
      exclude("junit-*.jar")
      exclude("hamcrest-*.jar")
      exclude("*-tests.jar")
      exclude("hbase-*.jar")
      exclude("datanucleus-*.jar")
      exclude("ant-*.jar")
      exclude("avro-*.jar")
      exclude("parquet-hadoop-bundle-*.jar")
      exclude("tephra-*.jar")
      exclude("twill-*.jar")
      exclude("hadoop-yarn-server-*.jar")
      exclude("jol-core-*.jar")
      exclude("jsr305-*.jar")
      exclude("spotbugs-annotations-*.jar")
      exclude("findbugs-annotations-*.jar")
    }
    into("$rootDir/distribution/package/catalogs/hive/libs/hive-metastore2-libs")
  }
}

tasks.withType<PublishToMavenLocal>().configureEach { enabled = false }
tasks.withType<PublishToMavenRepository>().configureEach { enabled = false }
