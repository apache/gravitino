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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.rel.SupportsPartitions;

public abstract class JdbcTablePartitionOperations implements TableOperations, SupportsPartitions {
  protected final DataSource dataSource;
  protected final JdbcTable loadedTable;

  protected JdbcTablePartitionOperations(DataSource dataSource, JdbcTable loadedTable) {
    checkArgument(dataSource != null, "dataSource is null");
    checkArgument(loadedTable != null, "loadedTable is null");
    this.dataSource = dataSource;
    this.loadedTable = loadedTable;
  }

  protected Connection getConnection(String databaseName) throws SQLException {
    Connection connection = dataSource.getConnection();
    connection.setCatalog(databaseName);
    return connection;
  }

  @Override
  public void close() throws IOException {
    // Nothing to be closed.
  }
}
