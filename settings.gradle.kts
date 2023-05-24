rootProject.name = "Unified Catalog"

include("api", "core", "schema", "server", "connectors")
include(":connectors:commons", ":connectors:connector-mysql")
