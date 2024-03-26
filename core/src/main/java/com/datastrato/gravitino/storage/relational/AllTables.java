/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

public class AllTables {
  public static final String METALAKE_TABLE_NAME = "metalake_meta";
  public static final String CATALOG_TABLE_NAME = "catalog_meta";
  public static final String SCHEMA_TABLE_NAME = "schema_meta";
  public static final String TABLE_TABLE_NAME = "table_meta";
  public static final String FILESET_TABLE_NAME = "fileset_meta";
  public static final String FILESET_VERSION_TABLE_NAME = "fileset_version_info";

  private AllTables() {
    // Prevent instantiation.
  }

  public static enum TABLE_NAMES {
    METALAKE_TABLE_NAME(AllTables.METALAKE_TABLE_NAME),
    CATALOG_TABLE_NAME(AllTables.CATALOG_TABLE_NAME),
    SCHEMA_TABLE_NAME(AllTables.SCHEMA_TABLE_NAME),
    TABLE_TABLE_NAME(AllTables.TABLE_TABLE_NAME),
    FILESET_TABLE_NAME(AllTables.FILESET_TABLE_NAME),
    FILESET_VERSION_TABLE_NAME(AllTables.FILESET_VERSION_TABLE_NAME);

    private final String tableName;

    TABLE_NAMES(String tableName) {
      this.tableName = tableName;
    }

    public String getTableName() {
      return this.tableName;
    }
  }
}
