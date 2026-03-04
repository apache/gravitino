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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.MetricsStorageException;
import org.apache.gravitino.utils.jdbc.JdbcDataSourceConfig;
import org.apache.gravitino.utils.jdbc.JdbcDataSourceFactory;

/** DataSource-backed JDBC connection provider with basic pooling defaults. */
public class DataSourceJdbcConnectionProvider {

  private final BasicDataSource dataSource;

  public DataSourceJdbcConnectionProvider(Map<String, String> jdbcProperties) {
    JdbcConnectionConfig config = new JdbcConnectionConfig(jdbcProperties);
    JdbcDataSourceConfig dataSourceConfig =
        new JdbcDataSourceConfig(
            config.jdbcUrl(),
            config.username(),
            config.password(),
            config.driverClassName(),
            config.maxTotal(),
            config.minIdle(),
            config.maxWaitMillis(),
            config.testOnBorrow(),
            JdbcDataSourceFactory.DEFAULT_VALIDATION_QUERY);
    this.dataSource = JdbcDataSourceFactory.create(dataSourceConfig);
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      throw new MetricsStorageException("Failed to close JDBC DataSource", e);
    }
  }
}
