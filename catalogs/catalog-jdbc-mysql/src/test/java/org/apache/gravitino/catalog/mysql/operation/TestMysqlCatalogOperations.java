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
package org.apache.gravitino.catalog.mysql.operation;

import java.sql.SQLException;
import org.apache.gravitino.catalog.jdbc.MySQLProtocolCompatibleCatalogOperations;
import org.apache.gravitino.catalog.mysql.MysqlCatalog;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TestMysqlCatalogOperations extends TestMysql {

  @Test
  public void testCheckJDBCDriver() throws SQLException {
    MySQLProtocolCompatibleCatalogOperations catalogOperations =
        new MySQLProtocolCompatibleCatalogOperations(
            null, null, DATABASE_OPERATIONS, TABLE_OPERATIONS, null);
    catalogOperations.initialize(getMySQLCatalogProperties(), null, new MysqlCatalog());
  }
}
