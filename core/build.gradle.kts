import net.ltgt.gradle.errorprone.errorprone

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
  id("idea")
  alias(libs.plugins.jcstress)
  alias(libs.plugins.jmh)
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":catalogs:catalog-common"))
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.metrics)
  implementation(libs.bundles.prometheus)
  implementation(libs.caffeine)
  implementation(libs.commons.dbcp2)
  implementation(libs.commons.io)
  implementation(libs.commons.lang3)
  implementation(libs.commons.collections4)
  implementation(libs.concurrent.trees)
  implementation(libs.guava)
  implementation(libs.h2db)
  implementation(libs.lance) {
    exclude(group = "com.fasterxml.jackson.core", module = "*") // provided by gravitino
    exclude(group = "com.fasterxml.jackson.datatype", module = "*") // provided by gravitino
    exclude(group = "commons-codec", module = "commons-codec") // provided by jcasbin
  }
  implementation(libs.mybatis)

  annotationProcessor(libs.lombok)

  compileOnly(libs.lombok)
  compileOnly(libs.servlet) // fix error-prone compile error

  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":server-common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(libs.awaitility)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)

  testRuntimeOnly(libs.junit.jupiter.engine)

  jcstressImplementation(libs.mockito.core)
}

tasks.test {
  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode == "embedded") {
    environment("GRAVITINO_HOME", project.rootDir.path)
  } else {
    environment("GRAVITINO_HOME", project.rootDir.path + "/distribution/package")
  }
}

tasks.withType<JavaCompile>().configureEach {
  if (name.contains("jcstress", ignoreCase = true)) {
    options.errorprone?.excludedPaths?.set(".*/generated/.*")
  }
}

tasks.named<JavaCompile>("jmhCompileGeneratedClasses").configure {
  options.errorprone?.isEnabled = false
  options.compilerArgs.removeAll { it.contains("Xplugin:ErrorProne") }
}

jcstress {
  /*
   Available modes:
   - sanity : takes seconds
   - quick : takes tens of seconds
   - default : takes minutes, good number of iterations
   - tough : takes tens of minutes, large number of iterations, most reliable
    */
  mode = "default"
  jvmArgsPrepend = "-Djdk.stdout.sync=true"
}

jmh {
  jmhVersion.set(libs.versions.jmh.asProvider())
  warmupIterations = 5
  iterations = 10
  fork = 1
  threads = 10
  resultFormat = "csv"
  resultsFile = file("$buildDir/reports/jmh/results.csv")
}
