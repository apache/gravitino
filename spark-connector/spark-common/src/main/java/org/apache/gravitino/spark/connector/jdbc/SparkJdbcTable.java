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
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.types.StructType;

public class SparkJdbcTable extends JDBCTable {

  private final GravitinoTableInfoHelper gravitinoTableInfoHelper;
  private final Table gravitinoTable;

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
}
