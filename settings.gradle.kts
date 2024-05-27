/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  id("org.gradle.toolchains.foojay-resolver-convention") version("0.7.0")
}

rootProject.name = "gravitino"

include("api", "common", "core", "meta", "server", "integration-test", "server-common")
include("catalogs:bundled-catalog")
include("catalogs:catalog-hive")
include("catalogs:catalog-lakehouse-iceberg")
include(
  "catalogs:catalog-jdbc-common",
  "catalogs:catalog-jdbc-doris",
  "catalogs:catalog-jdbc-mysql",
  "catalogs:catalog-jdbc-postgresql"
)
include("catalogs:catalog-hadoop")
include("catalogs:catalog-kafka")
include(
  "clients:client-java",
  "clients:client-java-runtime",
  "clients:filesystem-hadoop3",
  "clients:filesystem-hadoop3-runtime",
  "clients:client-python"
)
include("trino-connector")
include("spark-connector:spark-common", "spark-connector:spark33", "spark-connector:spark33-runtime", "spark-connector:spark34", "spark-connector:spark34-runtime", "spark-connector:spark35", "spark-connector:spark35-runtime")
project(":spark-connector:spark33").projectDir = file("spark-connector/v3.3/spark")
project(":spark-connector:spark33-runtime").projectDir = file("spark-connector/v3.3/spark-runtime")
project(":spark-connector:spark34").projectDir = file("spark-connector/v3.4/spark")
project(":spark-connector:spark34-runtime").projectDir = file("spark-connector/v3.4/spark-runtime")
project(":spark-connector:spark35").projectDir = file("spark-connector/v3.5/spark")
project(":spark-connector:spark35-runtime").projectDir = file("spark-connector/v3.5/spark-runtime")
include("flink-connector")
include("web")
include("docs")
include("integration-test-common")
