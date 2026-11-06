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

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.utils.GravitinoTableInfoHelper;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;

public class SparkJdbcTable extends JDBCTable {

  private static final String[] UNSAFE_OPTIONS = {
    "pushDownPredicate", "pushDownAggregate", "pushDownLimit", "pushDownOffset"
  };

  private final GravitinoTableInfoHelper gravitinoTableInfoHelper;
  private final Table gravitinoTable;
  private final boolean unsafePushDown;

  public SparkJdbcTable(
      Identifier identifier,
      Table gravitinoTable,
      JDBCTable jdbcTable,
      JDBCTableCatalog jdbcTableCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    this(
        identifier,
        gravitinoTable,
        jdbcTable,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter,
        hasUnsafePushDown(jdbcTable, gravitinoTable));
  }

  private SparkJdbcTable(
      Identifier identifier,
      Table gravitinoTable,
      JDBCTable jdbcTable,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter,
      boolean unsafePushDown) {
    super(
        identifier,
        jdbcTable.schema(),
        unsafePushDown ? safeReadOptions(jdbcTable.jdbcOptions()) : jdbcTable.jdbcOptions());
    this.unsafePushDown = unsafePushDown;
    this.gravitinoTable = gravitinoTable;
    this.gravitinoTableInfoHelper =
        new GravitinoTableInfoHelper(
            false,
            identifier,
            gravitinoTable,
            propertiesConverter,
            sparkTransformConverter,
            sparkTypeConverter);
  }

  /** {@inheritDoc} */
  @Override
  public JDBCScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return super.newScanBuilder(
        unsafePushDown
            ? new CaseInsensitiveStringMap(safeOptions(options.asCaseSensitiveMap()))
            : options);
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
    return gravitinoTableInfoHelper.schema();
  }

  @Override
  public Map<String, String> properties() {
    return gravitinoTableInfoHelper.properties();
  }

  private static boolean hasUnsafePushDown(JDBCTable table, Table source) {
    for (Column column : source.columns()) {
      Type type = column.dataType();
      if (type instanceof Types.StringType
          || type instanceof Types.VarCharType
          || type instanceof Types.FixedCharType
          || type instanceof Types.BinaryType
          || type instanceof Types.ExternalType
          || type instanceof Types.UUIDType) {
        return true;
      }
    }
    for (StructField field : table.schema().fields()) {
      if (DataTypes.TimestampNTZType.equals(field.dataType())) {
        // Spark 3.5 leaves LocalDateTime literals unquoted; Spark 4 quotes them. Check the
        // actual dialect instead of relying on a Spark version or provider name. Until the
        // dialect supports these literals, retain Spark evaluation for the whole table.
        return !(JdbcDialects.get(table.jdbcOptions().url())
                .compileValue(LocalDateTime.of(2000, 1, 1, 0, 0))
            instanceof String);
      }
    }
    return false;
  }

  private static Map<String, String> safeOptions(Map<String, String> original) {
    Map<String, String> safe = new HashMap<>(original);
    for (String key : UNSAFE_OPTIONS) {
      safe.keySet().removeIf(key::equalsIgnoreCase);
      safe.put(key, "false");
    }
    return safe;
  }

  private static JDBCOptions safeReadOptions(JDBCOptions options) {
    // Disable source evaluation on affected tables; the same query must retain Spark semantics.
    Map<String, String> original =
        JavaConverters.mapAsJavaMapConverter(options.parameters().originalMap()).asJava();
    // Fully qualified because java.util.Map is already imported.
    scala.collection.immutable.Map<String, String> parameters =
        new scala.collection.immutable.HashMap<>();
    for (Map.Entry<String, String> entry : safeOptions(original).entrySet()) {
      parameters = parameters.updated(entry.getKey(), entry.getValue());
    }
    return new JDBCOptions(parameters);
  }
}
