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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

/** PostgreSQL write validation before Spark creates or executes a JDBC write. */
public class TestSparkJdbcTableWriteValidation {

  @Test
  void testUnsupportedPostgreSqlColumnsFailBeforeWrite() {
    for (Type type :
        new Type[] {
          Types.UUIDType.get(),
          Types.ExternalType.of("jsonb"),
          Types.ExternalType.of("numeric"),
          Types.ExternalType.of("inet"),
          Types.ExternalType.of("interval")
        }) {
      SparkJdbcTable table = table(type, "jdbc:postgresql://localhost/test");
      // Reading remains supported; nullable columns are also rejected on write before any row is
      // bound.
      Assertions.assertEquals(DataTypes.StringType, table.schema().fields()[0].dataType());
      UnsupportedOperationException error =
          Assertions.assertThrows(
              UnsupportedOperationException.class, () -> table.newWriteBuilder(null));
      Assertions.assertTrue(error.getMessage().contains("special_value"));
      Assertions.assertTrue(error.getMessage().contains(type.simpleString()));
    }
  }

  @Test
  void testOrdinaryPostgreSqlTypesStillCreateWriteBuilder() {
    SparkJdbcTable table = table(Types.IntegerType.get(), "jdbc:postgresql://localhost/test");
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(info.options()).thenReturn(new CaseInsensitiveStringMap(Collections.emptyMap()));
    Assertions.assertNotNull(table.newWriteBuilder(info));
  }

  private SparkJdbcTable table(Type type, String url) {
    Table metadata = mock(Table.class);
    when(metadata.columns())
        .thenReturn(new Column[] {Column.of("special_value", type, null, true, false, null)});
    JDBCOptions options =
        new JDBCOptions(
            new HashMap<String, String>()
                .updated("url", url)
                .updated("dbtable", "t")
                .updated("driver", "org.postgresql.Driver"));
    JDBCTable physical = mock(JDBCTable.class);
    when(physical.schema()).thenReturn(new StructType());
    when(physical.jdbcOptions()).thenReturn(options);
    return new SparkJdbcTable(
        Identifier.of(new String[] {"public"}, "t"),
        metadata,
        physical,
        mock(JDBCTableCatalog.class),
        JdbcPropertiesConverter.getInstance(),
        new SparkTransformConverter(false),
        new SparkJdbcTypeConverter());
  }
}
