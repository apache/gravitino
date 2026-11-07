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

import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.utils.GravitinoTableInfoHelper;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkJdbcTable extends JDBCTable {

  private final GravitinoTableInfoHelper gravitinoTableInfoHelper;
  private final Table gravitinoTable;
  private final StructType sparkSchema;
  private final SparkJdbcPushDownPolicy pushDownPolicy;

  public SparkJdbcTable(
      Identifier identifier,
      Table gravitinoTable,
      JDBCTable jdbcTable,
      JDBCTableCatalog jdbcTableCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    super(identifier, jdbcTable.schema(), jdbcTable.jdbcOptions());
    this.gravitinoTable = gravitinoTable;
    this.gravitinoTableInfoHelper =
        new GravitinoTableInfoHelper(
            false,
            identifier,
            gravitinoTable,
            propertiesConverter,
            sparkTransformConverter,
            sparkTypeConverter);
    this.sparkSchema = alignTimestampArrays(gravitinoTableInfoHelper.schema(), jdbcTable.schema());
    this.pushDownPolicy =
        new SparkJdbcPushDownPolicy(
            gravitinoTable.columns(),
            jdbcTable.schema(),
            sparkSchema,
            JdbcDialects.get(jdbcTable.jdbcOptions().url()));
  }

  /**
   * Creates a scan builder that keeps unsafe expressions in Spark.
   *
   * @param options the scan options
   * @return the JDBC scan builder
   */
  @Override
  public JDBCScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkJdbcScanBuilder(super.newScanBuilder(options), pushDownPolicy);
  }

  /**
   * Validates PostgreSQL write types before Spark creates a JDBC writer.
   *
   * @param info the write information supplied by Spark
   * @return the JDBC write builder for supported column types
   */
  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (jdbcOptions().url().toLowerCase(Locale.ROOT).startsWith("jdbc:postgresql:")) {
      for (Column column : gravitinoTable.columns()) {
        if (column.dataType() instanceof Types.UUIDType
            || column.dataType() instanceof Types.ExternalType) {
          throw new UnsupportedOperationException(
              "Spark JDBC writes to PostgreSQL column '"
                  + column.name()
                  + "' of type "
                  + column.dataType().simpleString()
                  + " are not supported because Spark binds this type as VARCHAR; use a native PostgreSQL writer");
        }
      }
    }
    return super.newWriteBuilder(info);
  }

  @Override
  public String name() {
    return gravitinoTableInfoHelper.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return sparkSchema;
  }

  @Override
  public Map<String, String> properties() {
    return gravitinoTableInfoHelper.properties();
  }

  /**
   * Uses the reader's element type for timestamp arrays. Spark 3.5 reads PostgreSQL timestamp array
   * elements as TIMESTAMP even when top-level columns use TIMESTAMP_NTZ.
   */
  private static StructType alignTimestampArrays(StructType sparkSchema, StructType readerSchema) {
    StructField[] fields = sparkSchema.fields().clone();
    for (int i = 0; i < fields.length; i++) {
      DataType sparkType = fields[i].dataType();
      DataType readerType = readerType(readerSchema, fields[i].name());
      if (sparkType instanceof ArrayType
          && readerType instanceof ArrayType
          && isTimestamp(((ArrayType) sparkType).elementType())
          && isTimestamp(((ArrayType) readerType).elementType())) {
        fields[i] =
            new StructField(
                fields[i].name(),
                DataTypes.createArrayType(
                    ((ArrayType) readerType).elementType(), ((ArrayType) sparkType).containsNull()),
                fields[i].nullable(),
                fields[i].metadata());
      }
    }
    return new StructType(fields);
  }

  private static DataType readerType(StructType readerSchema, String name) {
    for (StructField field : readerSchema.fields()) {
      if (field.name().equalsIgnoreCase(name)) {
        return field.dataType();
      }
    }
    return null;
  }

  private static boolean isTimestamp(DataType type) {
    return DataTypes.TimestampType.equals(type) || DataTypes.TimestampNTZType.equals(type);
  }
}
