/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

public interface DatabaseOperation {

  /**
   * Initializes the database operations.
   *
   * @param dataSource The data source to use for the operations.
   * @param exceptionMapper The exception mapper to use for the operations.
   * @param conf The configuration for the operations.
   */
  void initialize(
      DataSource dataSource, JdbcExceptionConverter exceptionMapper, Map<String, String> conf);

  /**
   * Creates a database with the given name and comment.
   *
   * @param databaseName The name of the database.
   * @param comment The comment of the database.
   */
  void create(String databaseName, String comment, Map<String, String> properties)
      throws SchemaAlreadyExistsException;

  /**
   * @param databaseName The name of the database to check.
   * @param cascade If set to true, drops all the tables in the database as well.
   */
  boolean delete(String databaseName, boolean cascade);

  /** @return The list name of databases. */
  List<String> listDatabases();

  /**
   * @param databaseName The name of the database to check.
   * @return information object of the JDBC database.
   */
  JdbcSchema load(String databaseName) throws NoSuchSchemaException;
}
