rootProject.name = "Unified Catalog"

include("api", "core", "schema", "server")

pluginManagement {
  plugins {
    fun String.v() = extra["$this.version"].toString()
    fun PluginDependenciesSpec.idv(id: String, key: String = id) = id(id) version key.v()

    idv("com.google.protobuf")
    idv("org.jetbrains.gradle.plugin.idea-ext")
    kotlin("jvm") version "kotlin".v()
  }
}
