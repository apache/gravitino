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
package org.apache.gravitino.catalog.maxcompute.operation;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMaxComputeTableOperations extends TestMaxCompute {

  private static final Logger LOG = LoggerFactory.getLogger(TestMaxComputeTableOperations.class);

  @Test
  public void testListTableColumns() throws SQLException {
    List<String> databases = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(CollectionUtils.isEmpty(databases));

    String catalog = databases.get(1);
    String schemaPattern = null;
    String tableNamePattern = "%";
    String[] types = {"TABLE"};

    Connection connection = DATA_SOURCE.getConnection();
    DatabaseMetaData metaData = connection.getMetaData();

    ResultSet tablesResultSet = metaData.getTables(catalog, schemaPattern, tableNamePattern, types);
    Assertions.assertTrue(tablesResultSet.next());

    while (tablesResultSet.next()) {
      String tableName = tablesResultSet.getString("TABLE_NAME");

      ResultSet columnsResultSet = metaData.getColumns(catalog, schemaPattern, tableName, "%");

      while (columnsResultSet.next()) {
        String columnName = columnsResultSet.getString("COLUMN_NAME");
        String columnType = columnsResultSet.getString("TYPE_NAME");
        int columnSize = columnsResultSet.getInt("COLUMN_SIZE");
        int nullable = columnsResultSet.getInt("NULLABLE");
        String isNullable = nullable == 1 ? "YES" : "NO";
        LOG.info(
            "Column Name: %s,Column Type: %s,Column Size: %d,Nullable: %b",
            columnName, columnType, columnSize, isNullable);
      }
    }
  }
}
