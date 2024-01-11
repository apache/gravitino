/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.duckdb;

/** Implementation of a Duckdb catalog in Gravitino. */
public class DuckdbCatalog extends JdbcCatalog {

  @Override
  public String shortName() {
    return "jdbc-duckdb";
  }

}
