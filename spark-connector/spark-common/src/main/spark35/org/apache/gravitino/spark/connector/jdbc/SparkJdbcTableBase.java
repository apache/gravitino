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

package org.apache.gravitino.spark.connector.jdbc;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;

/**
 * Carries the {@code JDBCTable} constructor call for {@link SparkJdbcTable}. Spark 4.1 added a
 * fourth constructor parameter, so each Spark line has its own copy of this class while the table
 * itself stays shared.
 */
public abstract class SparkJdbcTableBase extends JDBCTable {

  /**
   * Creates the JDBC table this wrapper stands in for.
   *
   * @param identifier the table identifier
   * @param jdbcTable the JDBC table Spark loaded
   */
  protected SparkJdbcTableBase(Identifier identifier, JDBCTable jdbcTable) {
    super(identifier, jdbcTable.schema(), jdbcTable.jdbcOptions());
  }
}
