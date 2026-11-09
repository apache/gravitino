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

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.StructType;

/** Uses the Spark 3.4+ expression policy while retaining the common JDBC table behavior. */
class SparkJdbcTable34 extends SparkJdbcTable {
  SparkJdbcTable34(
      Identifier identifier,
      Table gravitinoTable,
      JDBCTable jdbcTable,
      JDBCTableCatalog jdbcTableCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter transformConverter,
      SparkTypeConverter typeConverter) {
    super(
        identifier,
        gravitinoTable,
        jdbcTable,
        jdbcTableCatalog,
        propertiesConverter,
        transformConverter,
        typeConverter);
  }

  @Override
  SparkJdbcPushDownPolicy createPushDownPolicy(
      Column[] columns,
      StructType readerSchema,
      StructType visibleSchema,
      JdbcDialect dialect,
      String jdbcUrl) {
    return new SparkJdbcPushDownPolicy34(columns, readerSchema, visibleSchema, dialect, jdbcUrl);
  }
}
