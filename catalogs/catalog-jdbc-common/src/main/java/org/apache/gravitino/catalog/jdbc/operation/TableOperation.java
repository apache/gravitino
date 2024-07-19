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
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

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
      Distribution distribution,
      Index[] indexes)
      throws TableAlreadyExistsException;

  /**
   * @param databaseName The name of the database.
   * @param tableName The name of the table.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  boolean drop(String databaseName, String tableName);

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
  boolean purge(String databaseName, String tableName);

  default JdbcTablePartitionOperations createJdbcTablePartitionOperations(JdbcTable loadedTable) {
    throw new UnsupportedOperationException("Table partition operation is not supported yet");
  }
}
