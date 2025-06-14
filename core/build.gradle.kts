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
import net.ltgt.gradle.errorprone.errorprone
plugins {
  `maven-publish`
  id("java")
  id("idea")
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
}

tasks.test {
  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode == "embedded") {
    environment("GRAVITINO_HOME", project.rootDir.path)
  } else {
    environment("GRAVITINO_HOME", project.rootDir.path + "/distribution/package")
  }
  val testMode = project.properties["testMode"] as? String ?: "embedded"
  if (testMode == "embedded") {
    environment("GRAVITINO_HOME", project.rootDir.path)
  } else {
    environment("GRAVITINO_HOME", project.rootDir.path + "/distribution/package")
  }

  // 新增：为Metaspace OOM测试配置JVM参数
  jvmArgs(
          "-Xmx4g",
          "-XX:MetaspaceSize=256m",
          "-XX:MaxMetaspaceSize=512m",
          "-XX:+UseG1GC",
          "-XX:+CMSClassUnloadingEnabled"
  )

  // 新增：测试分类配置
  useJUnitPlatform {
    if (project.hasProperty("runOptimizationTests")) {
      includeTags("optimization-test")
    } else {
      excludeTags("optimization-test", "performance-test", "stress-test")
    }

  }

tasks.withType<JavaCompile>().configureEach {
  if (name.contains("test", ignoreCase = true)) {
    options.errorprone?.excludedPaths?.set(".*/generated/.*")
  }
}

  tasks.register<Test>("unitTest") {
    description = "运行单元测试"
    group = "verification"

    useJUnitPlatform {
      excludeTags("integration-test", "performance-test", "stress-test")
    }

    include("**/TestClassLoaderPool.class")
    include("**/TestCatalogPropertyAnalyzer.class")
    include("**/TestRateLimiter.class")
    include("**/TestCatalogMetrics.class")
    include("**/TestCatalogConfigValidation.class")
  }

  tasks.register<Test>("integrationTest") {
    description = "运行集成测试"
    group = "verification"

    useJUnitPlatform {
      includeTags("integration-test")
      excludeTags("performance-test", "stress-test")
    }

    include("**/TestCatalogManagerOptimization.class")
    include("**/TestCatalogWrapperEnhancement.class")

    jvmArgs(
            "-Xmx6g",
            "-XX:MetaspaceSize=512m",
            "-XX:MaxMetaspaceSize=1g"
    )
  }

  tasks.register<Test>("performanceTest") {
    description = "运行性能基准测试"
    group = "verification"

    useJUnitPlatform {
      includeTags("performance-test")
    }

    include("**/TestCatalogPerformanceBenchmark.class")
    include("**/TestMetaspaceOOMScenario.class")

    jvmArgs(
            "-Xmx8g",
            "-XX:MetaspaceSize=1g",
            "-XX:MaxMetaspaceSize=2g",
            "-XX:+PrintGCDetails",
            "-XX:+PrintGCTimeStamps"
    )

    testLogging {
      events("passed", "skipped", "failed")
      showStandardStreams = true
    }
  }

  tasks.register<Test>("stressTest") {
    description = "运行压力测试"
    group = "verification"

    useJUnitPlatform {
      includeTags("stress-test")
    }

    include("**/TestCatalogOptimizationE2E.class")

    jvmArgs(
            "-Xmx12g",
            "-XX:MetaspaceSize=2g",
            "-XX:MaxMetaspaceSize=4g",
            "-XX:+UseG1GC",
            "-XX:+PrintGCDetails"
    )

    timeout.set(Duration.ofMinutes(30))
  }

  tasks.withType<Test> {
    reports {
      html.required.set(true)
      junitXml.required.set(true)
    }

    // 测试完成后生成内存使用报告
    doLast {
      if (project.hasProperty("generateMemoryReport")) {
        exec {
          commandLine("jstat", "-gc", "-t", "1", "1")
        }
      }
    }
  }

  // 测试覆盖率配置
  tasks.jacocoTestReport {
    reports {
      xml.required.set(true)
      html.required.set(true)
      csv.required.set(false)
    }

    executionData.setFrom(
            fileTree(layout.buildDirectory.dir("jacoco")).include("**/*.exec")
    )
  }
