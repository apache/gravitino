# Gravitino Project
Apache Gravitino is an open-source metadata lakehouse that unifies metadata across data lakes, warehouses, and catalogs. The core Java code lives under module src/ folders, with major components grouped under core, server, and connectors.


## Build & Commands
Fast Building: ./gradlew build -x web:web:build -x clients:client-python:build -x test -x spotlessCheck -x rat -x javadoc
Fix formatting: ./gradlew spotlessApply -x web:web:spotlessApply
Run unit tests: ./gradlew test -x web:web:test -x clients:client-python:test

Tip you can target specific modules directly, for example: ./gradlew :core:build :server:build
