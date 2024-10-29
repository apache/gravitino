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
  id("org.gradle.toolchains.foojay-resolver-convention") version("0.7.0")
}

rootProject.name = "gravitino"

val scalaVersion: String = gradle.startParameter.projectProperties["scalaVersion"]?.toString()
  ?: settings.extra["defaultScalaVersion"].toString()

include("api", "common", "core", "meta", "server", "server-common")
include("catalogs:catalog-common")
include("catalogs:catalog-hive")
include("catalogs:hive-metastore-common")
include("catalogs:catalog-lakehouse-iceberg")
include("catalogs:catalog-lakehouse-paimon")
include("catalogs:catalog-lakehouse-hudi")
include(
  "catalogs:catalog-jdbc-common",
  "catalogs:catalog-jdbc-doris",
  "catalogs:catalog-jdbc-mysql",
  "catalogs:catalog-jdbc-postgresql",
  "catalogs:catalog-jdbc-oceanbase"
)
include("catalogs:catalog-hadoop")
include("catalogs:catalog-kafka")
include(
  "clients:client-java",
  "clients:client-java-runtime",
  "clients:filesystem-hadoop3",
  "clients:filesystem-hadoop3-runtime",
  "clients:client-python",
  "clients:cli"
)
include("iceberg:iceberg-common")
include("iceberg:iceberg-rest-server")
include("authorizations:authorization-ranger")
include("trino-connector:trino-connector", "trino-connector:integration-test")
include("spark-connector:spark-common")
// kyuubi hive connector doesn't support 2.13 for Spark3.3
if (scalaVersion == "2.12") {
  include("spark-connector:spark-3.3", "spark-connector:spark-runtime-3.3")
  project(":spark-connector:spark-3.3").projectDir = file("spark-connector/v3.3/spark")
  project(":spark-connector:spark-runtime-3.3").projectDir = file("spark-connector/v3.3/spark-runtime")
  // flink only support scala 2.12
  include("flink-connector:flink")
  include("flink-connector:flink-runtime")
}
include("spark-connector:spark-3.4", "spark-connector:spark-runtime-3.4", "spark-connector:spark-3.5", "spark-connector:spark-runtime-3.5")
project(":spark-connector:spark-3.4").projectDir = file("spark-connector/v3.4/spark")
project(":spark-connector:spark-runtime-3.4").projectDir = file("spark-connector/v3.4/spark-runtime")
project(":spark-connector:spark-3.5").projectDir = file("spark-connector/v3.5/spark")
project(":spark-connector:spark-runtime-3.5").projectDir = file("spark-connector/v3.5/spark-runtime")
include("web:web", "web:integration-test")
include("docs")
include("integration-test-common")
include(":bundles:aws-bundle")
include(":bundles:gcp-bundle")
include(":bundles:aliyun-bundle")
