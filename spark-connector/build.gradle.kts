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
import com.diffplug.gradle.spotless.SpotlessExtension
import java.net.URI

// This project builds nothing of its own; it groups the per-Spark-version modules and owns the
// pieces they share. `spark-common` is a source tree rather than a module, so the version modules
// compile it themselves and nothing here produces a jar. Two exceptions stay enabled: Spotless,
// which formats the shared tree below, and downloadGlueHiveJars, which the version modules'
// Glue ITs depend on.
tasks.all {
  enabled = name.startsWith("spotless") || name == "downloadGlueHiveJars"
}

// spark-common belongs to no project, so no module's Spotless picks it up. Format it from here,
// where it lives, and keep the version modules scoped to their own trees.
plugins.withId("com.diffplug.spotless") {
  configure<SpotlessExtension> {
    java {
      target(project.fileTree("spark-common/src") { include("**/*.java") })
    }
  }
}

// Jars for Spark's IsolatedClientLoader: patched Hive 2.3.10 + Glue datacatalog client.
// aws-java-sdk-glue 1.12.31 requires PropertyNamingStrategy$PascalCaseStrategy which was
// removed in Jackson 2.12. Jackson included here so the isolated classloader uses a compatible
// version instead of the app-level Jackson bundled with Spark (2.14+).
// Only set when AWS_ACCESS_KEY_ID is present so version-specific modules can skip the download
// when Glue tests are not enabled.
val glueHiveJarsDir: String? =
  if (System.getenv("AWS_ACCESS_KEY_ID") != null) "$buildDir/tmp/glue-hive-jars" else null
extra["glueHiveJarsDir"] = glueHiveJarsDir
val glueLibsApiUrl =
  "https://api.github.com/repos/datastrato/spark-hive-glue-libs/contents/spark3/glue-3.4.0"

val downloadGlueHiveJars by
tasks.registering {
  glueHiveJarsDir?.let { outputs.dir(it) }
  onlyIf {
    val outputDir = file(glueHiveJarsDir ?: return@onlyIf false)
    outputDir.listFiles()?.none { it.name.endsWith(".jar") } ?: true
  }
  doLast {
    val outputDir = file(glueHiveJarsDir ?: return@doLast)
    outputDir.mkdirs()
    val response = URI(glueLibsApiUrl).toURL().readText()

    @Suppress("UNCHECKED_CAST")
    val entries = groovy.json.JsonSlurper().parseText(response) as List<Map<String, Any>>
    entries
      .filter { (it["name"] as String).endsWith(".jar") }
      .forEach { entry ->
        val jarName = entry["name"] as String
        val downloadUrl = entry["download_url"] as String
        val dest = outputDir.resolve(jarName)
        if (!dest.exists()) {
          logger.lifecycle("Downloading $jarName ...")
          URI(downloadUrl).toURL().openStream().use { input ->
            dest.outputStream().use { output -> input.copyTo(output) }
          }
        }
      }
  }
}
