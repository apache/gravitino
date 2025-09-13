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

package org.apache.gravitino.catalog.jdbc;

import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;

public class MySQLProtocolCompatibleCatalogOperations extends JdbcCatalogOperations {
  private static final int MYSQL_JDBC_DRIVER_MINIMAL_SUPPORT_VERSION = 8;

  public MySQLProtocolCompatibleCatalogOperations(
      JdbcExceptionConverter exceptionConverter,
      JdbcTypeConverter jdbcTypeConverter,
      JdbcDatabaseOperations databaseOperation,
      JdbcTableOperations tableOperation,
      JdbcColumnDefaultValueConverter columnDefaultValueConverter) {
    super(
        exceptionConverter,
        jdbcTypeConverter,
        databaseOperation,
        tableOperation,
        columnDefaultValueConverter);
  }

  @Override
  public void checkJDBCDriverVersion() {
    JDBCDriverInfo driverInfo = getDiverInfo();
    if (driverInfo.majorVersion < MYSQL_JDBC_DRIVER_MINIMAL_SUPPORT_VERSION) {
      throw new RuntimeException(
          String.format(
              "Mysql catalog does not support the jdbc driver version %s, minimal required version is 8.0",
              driverInfo.version));
    }
  }

  @Override
  public void close() {
    super.close();
  }
}
