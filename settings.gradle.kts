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
include("spark-connector:spark-connector", "spark-connector:spark-connector-runtime")
include("web")
include("docs")
include("integration-test-common")
