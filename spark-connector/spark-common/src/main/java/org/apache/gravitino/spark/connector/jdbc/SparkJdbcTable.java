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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkJdbcTable extends JDBCTable {

  private static final String[] UNSAFE_OPTIONS = {
    "pushDownPredicate", "pushDownAggregate", "pushDownLimit", "pushDownOffset"
  };

  private final GravitinoTableInfoHelper gravitinoTableInfoHelper;
  private final Table gravitinoTable;
  private final Runnable catalogCheck;
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
        jdbcTableCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter,
        () -> {});
  }

  /**
   * Creates a JDBC table with a guard against stale catalog configuration and credentials.
   *
   * @param identifier the table identifier
   * @param gravitinoTable the source metadata
   * @param jdbcTable the underlying Spark table
   * @param jdbcTableCatalog the Spark JDBC catalog
   * @param propertiesConverter the properties converter
   * @param sparkTransformConverter the transform converter
   * @param sparkTypeConverter the type converter
   * @param catalogCheck the driver-side check run before scan/write planning
   */
  public SparkJdbcTable(
      Identifier identifier,
      Table gravitinoTable,
      JDBCTable jdbcTable,
      JDBCTableCatalog jdbcTableCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter,
      Runnable catalogCheck) {
    super(identifier, jdbcTable.schema(), readOptions(jdbcTable, gravitinoTable));
    this.catalogCheck = catalogCheck;
    this.unsafePushDown = hasUnsafePushDown(gravitinoTable);

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
    catalogCheck.run();
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
    catalogCheck.run();
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

  private static boolean hasUnsafePushDown(Table source) {
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

  private static JDBCOptions readOptions(JDBCTable table, Table source) {
    if (!hasUnsafePushDown(source)) {
      return table.jdbcOptions();
    }
    // Disable source evaluation on affected tables; the same query must retain Spark semantics.
    Map<String, String> original =
        scala.collection.JavaConverters.mapAsJavaMapConverter(
                table.jdbcOptions().parameters().originalMap())
            .asJava();
    scala.collection.immutable.Map<String, String> parameters =
        new scala.collection.immutable.HashMap<>();
    for (Map.Entry<String, String> entry : safeOptions(original).entrySet()) {
      parameters = parameters.updated(entry.getKey(), entry.getValue());
    }
    return new JDBCOptions(parameters);
  }
}
