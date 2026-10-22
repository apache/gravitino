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

// CI stages for .github/workflows/presubmit.yml. Each stage is one task, so a CI job and a local
// run use the same command:
//
//   ./gradlew ciLint
//   ./gradlew ciSmallTest -PskipITs -PskipDockerTests=false
//   ./gradlew ciMediumTestJdbc -PskipITs -PskipDockerTests=false
//
// Until each test size has its own suite, a module's whole `test` task goes to one stage:
// modules whose `src/test` has Docker-tagged tests are medium, all others are small. A module
// with Docker-tagged tests that is missing below still runs in ciSmallTest with Docker on, so
// no test is dropped, and ciVerifyShards fails the lint stage until the module is listed.

// Medium shards: name -> modules whose `test` task needs Docker. Integration tests (under
// `integration/test/`) do not count; -PskipITs leaves them out of these stages.
val ciMediumShards: Map<String, List<String>> = mapOf(
  "hive" to listOf(":catalogs:hive-metastore-common"),
  "jdbc" to listOf(
    ":catalogs:catalog-jdbc-doris",
    ":catalogs:catalog-jdbc-mysql",
    ":catalogs:catalog-jdbc-postgresql",
    ":catalogs:catalog-jdbc-starrocks"
  ),
  "others" to listOf(
    ":catalogs:catalog-lakehouse-paimon",
    ":iceberg:iceberg-rest-server",
    ":maintenance:jobs",
    ":plugins:idp-basic"
  )
)

// Tests these stages do not run: core runs as lanes in the core-test job, the Python client in
// python-integration-test.yml, and the contrib catalogs below in contrib-catalog-test.yml.
val ciSkippedTestModules: Set<String> = setOf(
  ":core",
  ":clients:client-python",
  ":catalogs-contrib:catalog-jdbc-clickhouse",
  ":catalogs-contrib:catalog-jdbc-hologres",
  ":catalogs-contrib:catalog-jdbc-oceanbase"
)

val ciMediumTestModules: Set<String> = ciMediumShards.values.flatten().toSet()

// "others" is the catch-all medium shard. It may shrink freely; growing it past this baseline
// fails ciVerifyShards, so a new Docker module either joins a named shard or starts a new one.
val ciOthersShardBaseline = 4

val ciDockerTestTag = "gravitino-docker-test"

fun ciMediumTaskName(shard: String): String = "ciMediumTest" + shard.replaceFirstChar { it.uppercase() }

// Lint stage: static checks that compile nothing. Checkstyle tasks join when a module applies the
// plugin.
val ciLint = tasks.register("ciLint") {
  group = "verification"
  description = "Runs the lint CI stage: formatting, license headers and checkstyle."
  dependsOn(":rat", "ciVerifyShards")
}

tasks.register("ciVerifyShards") {
  group = "verification"
  description = "Fails if a module with Docker-tagged unit tests is in no medium shard, " +
    "or if the 'others' shard grows past its baseline."
  val testDirs = subprojects.associate { it.path to it.file("src/test") }
  val listed = ciMediumTestModules + ciSkippedTestModules
  val othersSize = ciMediumShards.getValue("others").size
  inputs.property("listed", listed)
  inputs.property("othersSize", othersSize)
  doLast {
    val unlisted = testDirs.filter { (path, dir) ->
      path !in listed && dir.walkTopDown().any {
        it.isFile && it.extension == "java" && "/integration/test/" !in it.invariantSeparatorsPath &&
          it.readText().contains(ciDockerTestTag)
      }
    }.keys.sorted()
    val problems = mutableListOf<String>()
    if (unlisted.isNotEmpty()) {
      problems += "Modules with '$ciDockerTestTag' unit tests are in no medium shard: " +
        "${unlisted.joinToString()}. Add each to a shard in gradle/ci-stages.gradle.kts."
    }
    if (othersSize > ciOthersShardBaseline) {
      problems += "The 'others' shard has $othersSize modules, over its baseline of " +
        "$ciOthersShardBaseline. Move modules to a named shard, or raise the baseline with a reason."
    }
    if (problems.isNotEmpty()) {
      throw GradleException(problems.joinToString("\n"))
    }
  }
}

val ciSmallTest = tasks.register("ciSmallTest") {
  group = "verification"
  description = "Runs the small CI stage: every module's test task except medium and skipped ones."
}

ciMediumShards.forEach { (shard, modules) ->
  tasks.register(ciMediumTaskName(shard)) {
    group = "verification"
    description = "Runs the '$shard' shard of the medium CI stage: ${modules.joinToString()}."
    dependsOn(modules.map { "$it:test" })
  }
}

subprojects {
  val moduleLints = tasks.matching { it.name == "spotlessCheck" || it.name.startsWith("checkstyle") }
  ciLint.configure { dependsOn(moduleLints) }

  if (path !in ciMediumTestModules && path !in ciSkippedTestModules) {
    // shadedJarTest tests the k8s plugin's shaded jar; it is a small test that `check` also runs.
    val moduleTests = tasks.matching { it.name == "test" || it.name == "shadedJarTest" }
    ciSmallTest.configure { dependsOn(moduleTests) }
  }
}

tasks.register("ciShards") {
  group = "help"
  description = "Writes the medium CI shards as JSON, to \$GITHUB_OUTPUT in GitHub Actions."
  val shardsJson = ciMediumShards.keys.joinToString(",", "[", "]") {
    "{\"shard\":\"$it\",\"task\":\"${ciMediumTaskName(it)}\"}"
  }
  val githubOutput = providers.environmentVariable("GITHUB_OUTPUT")
  doLast {
    logger.lifecycle("medium-shards=$shardsJson")
    if (githubOutput.isPresent) {
      File(githubOutput.get()).appendText("medium-shards=$shardsJson\n")
    }
  }
}
