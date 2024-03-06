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
include("catalogs:catalog-jdbc-common", "catalogs:catalog-jdbc-mysql", "catalogs:catalog-jdbc-postgresql")
include("catalogs:catalog-hadoop")
include("clients:client-java", "clients:client-java-runtime")
include("trino-connector")
include("spark-connector")
include("web")
include("docs")
