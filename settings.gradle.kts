/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
rootProject.name = "graviton"

include("api", "common", "core", "meta", "server", "integration-test")
include("catalogs:catalog-hive", "catalogs:catalog-lakehouse")
include("clients:client-java", "clients:client-java-runtime")
