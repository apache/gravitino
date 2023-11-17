/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.rel.TableChange;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

public interface TableOperation {

  /**
   * Initializes the table operations.
   *
   * @param dataSource The data source to use for the operations.
   * @param exceptionMapper The exception mapper to use for the operations.
   * @param jdbcTypeConverter The type converter to use for the operations.
   * @throws RuntimeException
   */
  void initialize(
      final DataSource dataSource,
      final JdbcExceptionConverter exceptionMapper,
      final JdbcTypeConverter jdbcTypeConverter)
      throws RuntimeException;

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @param columns The columns of the table.
   * @param comment The comment of the table.
   * @param properties The properties of the table.
   */
  void create(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties);

  /**
   * @param databaseName The name of the database to create.
   * @param tableName The name of the table to create.
   */
  void drop(String databaseName, String tableName);

  /**
   * @param databaseName The name of the database to create.
   * @return A list of table names in the database.
   */
  List<String> list(String databaseName);

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @return information object of the JDBC table.
   * @throws NoSuchSchemaException
   */
  JdbcTable load(String databaseName, String tableName) throws NoSuchSchemaException;

  /**
   * @param databaseName The name of the database.
   * @param oldTableName The name of the table to rename.
   * @param newTableName The new name of the table.
   */
  void rename(String databaseName, String oldTableName, String newTableName);

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @param changes The changes to apply to the table.
   */
  void alterTable(String databaseName, String tableName, TableChange... changes);

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   */
  void purge(String databaseName, String tableName);
}
