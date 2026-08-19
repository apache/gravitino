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
package org.apache.gravitino.spark.connector.jdbc.doris;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.doris.spark.catalog.DorisTableCatalog;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.util.SchemaConvertors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.collection.immutable.Map$;

/** Spark 3.5 bridge around the official Doris table catalog and Spark JDBC V2. */
@SuppressWarnings("overrides")
final class DorisTableCatalog35 extends DorisTableCatalog {

  DorisPhysicalSchema35 loadPhysicalSchema(Identifier identifier) {
    if (identifier.namespace().length != 1) {
      throw new IllegalArgumentException("Doris table identifiers require one schema");
    }
    try {
      Schema schema = frontend().getTableSchema(identifier.namespace()[0], identifier.name());
      List<StructField> fields = new ArrayList<>(schema.size());
      List<String> typeNames = new ArrayList<>(schema.size());
      List<Boolean> catalystTypesResolved = new ArrayList<>(schema.size());
      List<Boolean> nullabilityKnown = new ArrayList<>(schema.size());
      for (Field field : schema.getProperties()) {
        DataType dataType;
        boolean catalystTypeResolved;
        try {
          dataType =
              SchemaConvertors.toCatalystType(
                  field.getType(), field.getPrecision(), field.getScale());
          catalystTypeResolved = true;
        } catch (Exception e) {
          // The Doris REST Field model does not expose nullability, and an unrecognized type must
          // be carried as unresolved metadata so the compatibility planner can fail closed.
          dataType = DataTypes.StringType;
          catalystTypeResolved = false;
        }
        fields.add(DorisPhysicalSchema35.createUnknownNullableField(field.getName(), dataType));
        typeNames.add(typeNameWithParameters(field));
        catalystTypesResolved.add(catalystTypeResolved);
        nullabilityKnown.add(false);
      }
      return new DorisPhysicalSchema35(
          DataTypes.createStructType(fields), typeNames, catalystTypesResolved, nullabilityKnown);
    } catch (Exception e) {
      throw physicalSchemaLoadFailure(identifier, e);
    }
  }

  static IllegalArgumentException physicalSchemaLoadFailure(
      Identifier identifier, Throwable ignoredFailure) {
    return new IllegalArgumentException("Failed to load Doris physical schema for " + identifier);
  }

  Table createJdbcTable(
      Identifier identifier,
      DorisReadSchema35 readSchema,
      String jdbcUrl,
      String jdbcDriver,
      String jdbcUser,
      String jdbcPassword,
      DorisJdbcReadOptions35 readOptions) {
    return new JDBCTable(
        identifier,
        readSchema.schema(),
        jdbcOptions(
            jdbcUrl,
            readSchema.tableOrQuery(identifier),
            jdbcDriver,
            jdbcUser,
            jdbcPassword,
            readOptions));
  }

  private static String typeNameWithParameters(Field field) {
    String typeName = field.getType();
    if (typeName == null || typeName.indexOf('(') >= 0) {
      return typeName;
    }
    if (typeName.toLowerCase(Locale.ROOT).startsWith("decimal") && field.getPrecision() > 0) {
      return String.format(
          Locale.ROOT, "%s(%d,%d)", typeName, field.getPrecision(), field.getScale());
    }
    return typeName;
  }

  private static JDBCOptions jdbcOptions(
      String jdbcUrl,
      String tableOrQuery,
      String jdbcDriver,
      String jdbcUser,
      String jdbcPassword,
      DorisJdbcReadOptions35 readOptions) {
    scala.collection.immutable.Map<String, String> parameters = Map$.MODULE$.empty();
    parameters = parameters.$plus(new Tuple2<>("driver", jdbcDriver));
    parameters = parameters.$plus(new Tuple2<>("user", jdbcUser));
    parameters = parameters.$plus(new Tuple2<>("password", jdbcPassword));
    for (Map.Entry<String, String> entry : readOptions.asSparkOptions().entrySet()) {
      parameters = parameters.$plus(new Tuple2<>(entry.getKey(), entry.getValue()));
    }
    return new JDBCOptions(jdbcUrl, tableOrQuery, parameters);
  }
}
