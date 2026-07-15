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

import com.github.gradle.node.NodeExtension
import com.github.gradle.node.npm.task.NpxTask

configure<NodeExtension> {
  version = "21.6.1"
  npmVersion = "10.2.4"
  download = true
}

tasks {
  val openApiSource = project.file("open-api/openapi.yaml")
  val openApiBundle = project.file("open-api/default/openapi.json")

  val bundleOpenApi by registering(NpxTask::class) {
    group = "documentation"
    description = "Bundles the default OpenAPI specification as JSON."
    command.set("@redocly/cli@1.23.1")
    args.set(
      listOf(
        "bundle",
        openApiSource.absolutePath,
        "--output",
        openApiBundle.absolutePath
      )
    )
    inputs.files(fileTree("open-api") { include("*.yaml") })
    outputs.file(openApiBundle)

    doFirst { openApiBundle.parentFile.mkdirs() }
  }

  val lintOpenAPI by registering(NpxTask::class) {
    command.set("@redocly/cli@1.23.1")
    args.set(listOf("lint", "--extends=recommended-strict", "${project.projectDir}/open-api/openapi.yaml"))
  }

  val lintIdpOpenAPI by registering(NpxTask::class) {
    command.set("@redocly/cli@1.23.1")
    args.set(
      listOf(
        "lint",
        "--extends=recommended-strict",
        "${project.projectDir}/open-api/idp/openapi.yaml"
      )
    )
  }

  build {
    dependsOn(bundleOpenApi)
    dependsOn(lintOpenAPI)
    dependsOn(lintIdpOpenAPI)
  }
}
