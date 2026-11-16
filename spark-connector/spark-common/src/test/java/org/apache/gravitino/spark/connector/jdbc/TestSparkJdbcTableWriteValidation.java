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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

/** PostgreSQL write validation before Spark creates or executes a JDBC write. */
public class TestSparkJdbcTableWriteValidation {

  @Test
  void testTimestampArraysUseTheReaderElementType() {
    Type timestamps = Types.ListType.nullable(Types.TimestampType.withoutTimeZone());
    // Spark 3.5 reads timestamp array elements as TIMESTAMP even with preferTimestampNTZ.
    SparkJdbcTable spark35 = table(timestamps, DataTypes.createArrayType(DataTypes.TimestampType));
    Assertions.assertEquals(
        DataTypes.createArrayType(DataTypes.TimestampType),
        spark35.schema().fields()[0].dataType());
    SparkJdbcTable spark40 =
        table(timestamps, DataTypes.createArrayType(DataTypes.TimestampNTZType));
    Assertions.assertEquals(
        DataTypes.createArrayType(DataTypes.TimestampNTZType),
        spark40.schema().fields()[0].dataType());
    // Top-level columns keep the converter's type.
    Assertions.assertEquals(
        DataTypes.TimestampNTZType,
        table(Types.TimestampType.withoutTimeZone(), DataTypes.TimestampType)
            .schema()
            .fields()[0]
            .dataType());
  }

  @Test
  void testTimestampArraysKeepCaseDistinctReaderTypes() {
    Column lower = Column.of("ts", Types.ListType.nullable(Types.TimestampType.withoutTimeZone()));
    Column upper = Column.of("TS", Types.ListType.nullable(Types.TimestampType.withTimeZone()));
    DataType ntzArray = DataTypes.createArrayType(DataTypes.TimestampNTZType);
    DataType zonedArray = DataTypes.createArrayType(DataTypes.TimestampType);
    StructType readerSchema = new StructType().add("ts", ntzArray).add("TS", zonedArray);
    for (Column[] columns : new Column[][] {{lower, upper}, {upper, lower}}) {
      for (StructType ordered :
          new StructType[] {
            readerSchema,
            new StructType().add(readerSchema.fields()[1]).add(readerSchema.fields()[0])
          }) {
        SparkJdbcTable table = table(columns, "jdbc:postgresql://localhost/test", ordered);
        Assertions.assertEquals(ntzArray, table.schema().apply("ts").dataType());
        Assertions.assertEquals(zonedArray, table.schema().apply("TS").dataType());
      }
    }
  }

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
    return table(type, url, new SparkJdbcTypeConverter(true).toSparkType(type));
  }

  private SparkJdbcTable table(Type type, DataType readerType) {
    return table(type, "jdbc:postgresql://localhost/test", readerType);
  }

  private SparkJdbcTable table(Type type, String url, DataType readerType) {
    return table(
        new Column[] {Column.of("special_value", type, null, true, false, null)},
        url,
        new StructType().add("special_value", readerType));
  }

  private SparkJdbcTable table(Column[] columns, String url, StructType readerSchema) {
    Table metadata = mock(Table.class);
    when(metadata.columns()).thenReturn(columns);
    JDBCOptions options =
        new JDBCOptions(
            new HashMap<String, String>()
                .updated("url", url)
                .updated("dbtable", "t")
                .updated("driver", "org.postgresql.Driver"));
    JDBCTable physical = mock(JDBCTable.class);
    when(physical.schema()).thenReturn(readerSchema);
    when(physical.jdbcOptions()).thenReturn(options);
    return new SparkJdbcTable(
        Identifier.of(new String[] {"public"}, "t"),
        metadata,
        physical,
        mock(JDBCTableCatalog.class),
        JdbcPropertiesConverter.getInstance(),
        new SparkTransformConverter(false),
        new SparkJdbcTypeConverter(true));
  }
}
