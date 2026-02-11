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

include("api", "common", "core", "server", "server-common")
include("catalogs:catalog-common")
include("catalogs:catalog-hive")
include("catalogs:hive-metastore-common")
include("catalogs:hive-metastore2-libs", "catalogs:hive-metastore3-libs")
include("catalogs:catalog-lakehouse-iceberg")
include("catalogs:catalog-lakehouse-paimon")
include("catalogs:catalog-lakehouse-hudi")
include("catalogs:catalog-lakehouse-generic")
include(
  "catalogs:catalog-jdbc-common",
  "catalogs:catalog-jdbc-doris",
  "catalogs:catalog-jdbc-mysql",
  "catalogs:catalog-jdbc-postgresql",
  "catalogs:catalog-jdbc-starrocks"
)

include("catalogs:catalog-fileset")
include("catalogs:catalog-kafka")
include("catalogs:catalog-model")

include("catalogs-contrib:catalog-jdbc-clickhouse")
include("catalogs-contrib:catalog-jdbc-oceanbase")

include(
  "clients:client-java",
  "clients:client-java-runtime",
  "clients:filesystem-hadoop3",
  "clients:filesystem-hadoop3-runtime",
  "clients:client-python",
  "clients:cli"
)
if (gradle.startParameter.projectProperties["enableFuse"]?.toBoolean() == true) {
  include("clients:filesystem-fuse")
} else {
  println("Skipping filesystem-fuse module since enableFuse is set to false")
}
include("iceberg:iceberg-common")
include("iceberg:iceberg-rest-server")
include("lance:lance-common")
include("lance:lance-rest-server")
include("authorizations:authorization-ranger", "authorizations:authorization-common", "authorizations:authorization-chain")
include("trino-connector:trino-connector", "trino-connector:integration-test")
include("spark-connector:spark-common")
if (scalaVersion == "2.12") {
  // flink only support scala 2.12
  include("flink-connector:flink")
  include("flink-connector:flink-runtime")
}
include("spark-connector:spark-3.3", "spark-connector:spark-runtime-3.3")
project(":spark-connector:spark-3.3").projectDir = file("spark-connector/v3.3/spark")
project(":spark-connector:spark-runtime-3.3").projectDir = file("spark-connector/v3.3/spark-runtime")
include("spark-connector:spark-3.4", "spark-connector:spark-runtime-3.4", "spark-connector:spark-3.5", "spark-connector:spark-runtime-3.5")
project(":spark-connector:spark-3.4").projectDir = file("spark-connector/v3.4/spark")
project(":spark-connector:spark-runtime-3.4").projectDir = file("spark-connector/v3.4/spark-runtime")
project(":spark-connector:spark-3.5").projectDir = file("spark-connector/v3.5/spark")
project(":spark-connector:spark-runtime-3.5").projectDir = file("spark-connector/v3.5/spark-runtime")
include("web:web", "web:integration-test")
include("web-v2:web", "web-v2:integration-test")
include("docs")
include("integration-test-common")
include(":bundles:aws", ":bundles:aws-bundle", ":bundles:iceberg-aws-bundle")
include(":bundles:gcp", ":bundles:gcp-bundle", ":bundles:iceberg-gcp-bundle")
include(":bundles:aliyun", ":bundles:aliyun-bundle", ":bundles:iceberg-aliyun-bundle")
include(":bundles:azure", ":bundles:azure-bundle", ":bundles:iceberg-azure-bundle")
include(":catalogs:hadoop-common")
include(":lineage")
include(":mcp-server")
include(":maintenance:optimizer", ":maintenance:jobs")
