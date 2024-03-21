/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import java.util.Map;

/** Table operations for Doris. */
public class DorisTableOperations extends JdbcTableOperations {
  // TODO: add implementation for doris catalog

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
    throw new UnsupportedOperationException();
  }
}
