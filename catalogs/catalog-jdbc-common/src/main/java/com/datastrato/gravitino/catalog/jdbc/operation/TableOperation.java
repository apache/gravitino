/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
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
   * @param conf The configuration to use for the operations.
   */
  void initialize(
      DataSource dataSource,
      JdbcExceptionConverter exceptionMapper,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcColumnDefaultValueConverter jdbcColumnDefaultValueConverter,
      Map<String, String> conf);

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @param columns The columns of the table.
   * @param comment The comment of the table.
   * @param properties The properties of the table.
   * @param partitioning The partitioning of the table.
   * @param indexes The indexes of the table.
   */
  void create(
      String databaseName,
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Index[] indexes)
      throws TableAlreadyExistsException;

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   */
  void drop(String databaseName, String tableName) throws NoSuchTableException;

  /**
   * @param databaseName The name of the database.
   * @return A list of table names in the database.
   */
  List<String> listTables(String databaseName) throws NoSuchSchemaException;

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @return information object of the JDBC table.
   * @throws NoSuchTableException
   */
  JdbcTable load(String databaseName, String tableName) throws NoSuchTableException;

  /**
   * @param databaseName The name of the database.
   * @param oldTableName The name of the table to rename.
   * @param newTableName The new name of the table.
   */
  void rename(String databaseName, String oldTableName, String newTableName)
      throws NoSuchTableException;

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @param changes The changes to apply to the table.
   */
  void alterTable(String databaseName, String tableName, TableChange... changes)
      throws NoSuchTableException;

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   */
  void purge(String databaseName, String tableName) throws NoSuchTableException;
}
