/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.catalog.jdbc.operation;

import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;

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
   * @param properties Additional properties for the database creation.
   */
  void create(String databaseName, String comment, Map<String, String> properties)
      throws SchemaAlreadyExistsException;

  /**
   * @param databaseName The name of the database to check.
   * @param cascade If set to true, drops all the tables in the database as well.
   * @return true if the database is successfully deleted; false if the database does not exist.
   */
  boolean delete(String databaseName, boolean cascade);

  /**
   * @return The list name of databases.
   */
  List<String> listDatabases();

  /**
   * Checks if the specified database exists.
   *
   * @param databaseName The name of the database to check.
   * @return true if the database exists; false otherwise.
   */
  boolean exist(String databaseName);

  /**
   * @param databaseName The name of the database to check.
   * @return information object of the JDBC database.
   */
  JdbcSchema load(String databaseName) throws NoSuchSchemaException;
}
