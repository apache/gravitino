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
include("spark-connector:spark-common", "spark-connector:spark-3.3", "spark-connector:spark-runtime-3.3", "spark-connector:spark-3.4", "spark-connector:spark-runtime-3.4", "spark-connector:spark-3.5", "spark-connector:spark-runtime-3.5")
project(":spark-connector:spark-3.3").projectDir = file("spark-connector/v3.3/spark")
project(":spark-connector:spark-runtime-3.3").projectDir = file("spark-connector/v3.3/spark-runtime")
project(":spark-connector:spark-3.4").projectDir = file("spark-connector/v3.4/spark")
project(":spark-connector:spark-runtime-3.4").projectDir = file("spark-connector/v3.4/spark-runtime")
project(":spark-connector:spark-3.5").projectDir = file("spark-connector/v3.5/spark")
project(":spark-connector:spark-runtime-3.5").projectDir = file("spark-connector/v3.5/spark-runtime")
include("web")
include("docs")
include("integration-test-common")
