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

// Per-module ratchet over the buildHealth report (issue #14106). Advice is warn-only; this check
// fails only when a module has more findings than its entry in gradle/build-health-baseline.json.
// A module missing from the baseline is allowed 0 findings, so new modules start clean.
//
//   ./gradlew checkBuildHealthBaseline   compare counts with the baseline, write a Markdown summary
//   ./gradlew updateBuildHealthBaseline  rewrite the baseline from the current counts
//                                        (-PbuildHealthBaselineMargin=0.1 adds a 10% allowance)

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import kotlin.math.ceil

val reportDir = layout.buildDirectory.dir("reports/dependency-analysis")
val reportFile = reportDir.map { it.file("build-health-report.json") }
val summaryFile = reportDir.map { it.file("build-health-summary.md") }
val baselineFile = rootProject.file("gradle/build-health-baseline.json")

data class ModuleCount(
  val path: String,
  val usedTransitive: Int,
  val unused: Int,
  val incorrectConfiguration: Int,
  val other: Int
) {
  val total get() = usedTransitive + unused + incorrectConfiguration + other
}

// Dependency advice with only a target configuration is an undeclared (used transitive)
// dependency, with only a source configuration an unused one, and with both a wrong configuration.
fun readCounts(file: File): Map<String, ModuleCount> {
  val report = JsonSlurper().parse(file) as Map<*, *>
  return (report["projectAdvice"] as List<*>).map { it as Map<*, *> }.associate { advice ->
    val deps = (advice["dependencyAdvice"] as? List<*>).orEmpty().map { it as Map<*, *> }
    val path = advice["projectPath"] as String
    fun count(from: Boolean, to: Boolean) =
      deps.count {
        (it["fromConfiguration"] != null) == from && (it["toConfiguration"] != null) == to
      }
    path to ModuleCount(
      path = path,
      usedTransitive = count(from = false, to = true),
      unused = count(from = true, to = false),
      incorrectConfiguration = count(from = true, to = true),
      other = (advice["pluginAdvice"] as? List<*>).orEmpty().size +
        (advice["moduleAdvice"] as? List<*>).orEmpty().size
    )
  }
}

fun readBaseline(): Map<String, Int> =
  if (baselineFile.exists()) {
    (JsonSlurper().parse(baselineFile) as Map<*, *>).entries
      .associate { (path, max) -> path as String to (max as Number).toInt() }
  } else {
    emptyMap()
  }

fun statusOf(count: Int, max: Int) = when {
  count > max -> "over"
  count == max -> "at"
  else -> "below"
}

tasks.register("checkBuildHealthBaseline") {
  group = "verification"
  description = "Fails if a module has more buildHealth findings than its baseline allows."
  dependsOn("buildHealth")
  doLast {
    val counts = readCounts(reportFile.get().asFile)
    val baseline = readBaseline()
    val rows = (counts.keys + baseline.keys).toSortedSet().map { path ->
      Triple(path, counts[path]?.total ?: 0, baseline[path] ?: 0)
    }.filter { (_, count, max) -> count > 0 || max > 0 }
    val statusOrder = listOf("over", "below", "at")
    val sorted = rows.sortedWith(
      compareBy<Triple<String, Int, Int>> { statusOrder.indexOf(statusOf(it.second, it.third)) }
        .thenByDescending { it.second }
        .thenBy { it.first }
    )
    val over = sorted.filter { it.second > it.third }
    val below = sorted.filter { it.second < it.third }

    val md = StringBuilder()
    md.appendLine("## Dependency health (buildHealth)").appendLine()
    md.appendLine("| Findings | Baseline | Modules over | Modules below (cleanup) |")
    md.appendLine("| ---: | ---: | ---: | ---: |")
    val totals = listOf(rows.sumOf { it.second }, baseline.values.sum(), over.size, below.size)
    md.appendLine(totals.joinToString(" | ", "| ", " |"))
    md.appendLine()
    if (over.isNotEmpty()) {
      md.appendLine("### Over baseline").appendLine()
      over.forEach { (path, count, max) ->
        val note = if (path !in baseline) " (not in baseline, so allowed 0)" else ""
        md.appendLine("- `$path`: $count findings, baseline $max$note.")
        md.appendLine("  Details: `./gradlew $path:projectHealth`")
      }
      md.appendLine()
      md.appendLine("Fix the advice, or regenerate the baseline:")
      md.appendLine("`./gradlew updateBuildHealthBaseline`.")
      md.appendLine()
    }
    md.appendLine("| Module | Findings | Baseline | Status |")
    md.appendLine("| --- | ---: | ---: | --- |")
    sorted.forEach { (path, count, max) ->
      md.appendLine("| `$path` | $count | $max | ${statusOf(count, max)} |")
    }
    summaryFile.get().asFile.writeText(md.toString())

    sorted.forEach { (path, count, max) ->
      val c = counts[path]
      val detail = c?.let {
        " [used transitive ${it.usedTransitive}, unused ${it.unused}, " +
          "incorrect configuration ${it.incorrectConfiguration}, other ${it.other}]"
      }.orEmpty()
      val hint = if (count < max) " -> lower it: ./gradlew updateBuildHealthBaseline" else ""
      logger.lifecycle("buildHealth $path: $count / $max ${statusOf(count, max)}$detail$hint")
    }
    logger.lifecycle("buildHealth summary: ${summaryFile.get().asFile}")
    if (over.isNotEmpty()) {
      throw GradleException(
        "buildHealth findings exceed the baseline in ${over.size} module(s): " +
          over.joinToString { (path, count, max) ->
            "$path has $count, baseline $max" +
              if (path !in baseline) " (missing from baseline, so 0)" else ""
          } +
          ". Run ./gradlew <module>:projectHealth for details, fix the advice, or regenerate " +
          "the baseline with ./gradlew updateBuildHealthBaseline."
      )
    }
  }
}

tasks.register("updateBuildHealthBaseline") {
  group = "verification"
  description = "Rewrites gradle/build-health-baseline.json from the current buildHealth counts."
  dependsOn("buildHealth")
  doLast {
    val margin = (findProperty("buildHealthBaselineMargin") ?: "0").toString().toDouble()
    val baseline = readCounts(reportFile.get().asFile).values
      .filter { it.total > 0 }
      .sortedBy { it.path }
      .associate { it.path to ceil(it.total * (1 + margin) - 1e-9).toInt() }
    baselineFile.writeText(JsonOutput.prettyPrint(JsonOutput.toJson(baseline)) + "\n")
    val allowed = baseline.values.sum()
    logger.lifecycle("Wrote ${baseline.size} modules ($allowed findings) to $baselineFile")
  }
}
