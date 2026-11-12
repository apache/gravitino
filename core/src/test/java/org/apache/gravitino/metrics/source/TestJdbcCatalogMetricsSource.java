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

package org.apache.gravitino.metrics.source;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcCatalogMetricsSource {

  @Test
  public void testRegisterDatasourceMetricsWithNonBasicDataSource() {
    JdbcCatalogMetricsSource source = new JdbcCatalogMetricsSource("metalake", "catalog");

    DataSource nonBasicDataSource =
        new DataSource() {
          @Override
          public Connection getConnection() {
            return null;
          }

          @Override
          public Connection getConnection(String username, String password) {
            return null;
          }

          @Override
          public PrintWriter getLogWriter() {
            return null;
          }

          @Override
          public void setLogWriter(PrintWriter out) {}

          @Override
          public void setLoginTimeout(int seconds) {}

          @Override
          public int getLoginTimeout() {
            return 0;
          }

          @Override
          public java.util.logging.Logger getParentLogger() {
            return java.util.logging.Logger.getGlobal();
          }

          @Override
          public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not a wrapper");
          }

          @Override
          public boolean isWrapperFor(Class<?> iface) {
            return false;
          }
        };

    Assertions.assertDoesNotThrow(() -> source.registerDatasourceMetrics(nonBasicDataSource));
  }
}
