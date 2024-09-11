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
package org.apache.gravitino.catalog.oceanbase.operation;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/** Table operations for OceanBase. */
public class OceanBaseTableOperations extends JdbcTableOperations {

  @Override
  public List<String> listTables(String databaseName) throws NoSuchSchemaException {
    final List<String> names = Lists.newArrayList();

    try (Connection connection = getConnection(databaseName);
        ResultSet tables = getTables(connection)) {
      while (tables.next()) {
        if (Objects.equals(tables.getString("TABLE_CAT"), databaseName)) {
          names.add(tables.getString("TABLE_NAME"));
        }
      }
      LOG.info("Finished listing tables size {} for database name {} ", names.size(), databaseName);
      return names;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public JdbcTable load(String databaseName, String tableName) throws NoSuchTableException {
    return super.load(databaseName, tableName.toLowerCase());
  }

  @Override
  protected String generateCreateTableSql(
      String tableName,
      JdbcColumn[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      Index[] indexes) {
    if (ArrayUtils.isNotEmpty(partitioning)) {
      throw new UnsupportedOperationException("Currently not support Partition tables.");
    }

    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  protected boolean getAutoIncrementInfo(ResultSet resultSet) throws SQLException {
    return "YES".equalsIgnoreCase(resultSet.getString("IS_AUTOINCREMENT"));
  }

  @Override
  protected Map<String, String> getTableProperties(Connection connection, String tableName)
      throws SQLException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  protected String generateRenameTableSql(String oldTableName, String newTableName) {
    return String.format("RENAME TABLE `%s` TO `%s`", oldTableName, newTableName);
  }

  @Override
  protected String generateDropTableSql(String tableName) {
    return String.format("DROP TABLE `%s`", tableName);
  }

  @Override
  protected String generatePurgeTableSql(String tableName) {
    return String.format("TRUNCATE TABLE `%s`", tableName);
  }

  @Override
  public void alterTable(String databaseName, String tableName, TableChange... changes)
      throws NoSuchTableException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  protected String generateAlterTableSql(
      String databaseName, String tableName, TableChange... changes) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  protected JdbcTable getOrCreateTable(
      String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
    return null != lazyLoadCreateTable ? lazyLoadCreateTable : load(databaseName, tableName);
  }
}
