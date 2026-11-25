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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.doris.spark.catalog.DorisTable;
import org.apache.doris.spark.client.DorisFrontendClient;
import org.apache.doris.spark.config.DorisConfig;
import org.apache.doris.spark.config.DorisOptions;
import org.apache.doris.spark.rest.models.Field;
import org.apache.doris.spark.util.SchemaConvertors;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/** Creates the official Doris table without exposing its write capabilities. */
final class DorisNativeTableFactory35 {
  private DorisNativeTableFactory35() {}

  @Nullable
  static Table create(
      Identifier identifier,
      StructType schema,
      Map<String, String> nativeOptions,
      JdbcCredential credential) {
    if (identifier.namespace().length != 1
        || identifier.namespace()[0].contains(".")
        || identifier.name().contains(".")
        || identifier.namespace()[0].contains("`")
        || identifier.name().contains("`")) {
      throw new IllegalArgumentException("Unsupported identifier for a Doris native scan");
    }
    Map<String, String> options = new HashMap<>(nativeOptions);
    options.put("doris.table.identifier", identifier.namespace()[0] + "." + identifier.name());
    options.put("doris.user", credential.jdbcUser());
    options.put("doris.password", credential.jdbcPassword());
    try {
      DorisConfig config = DorisConfig.fromMap(options, false);
      // The native reader matches projected names exactly. The JDBC baseline permits certain
      // aliases and case differences, so compare the connector's physical schema first.
      DorisFrontendClient frontend = new DorisFrontendClient(config);
      StructType physical;
      try {
        List<Field> columns =
            frontend.getTableSchema(identifier.namespace()[0], identifier.name()).getProperties();
        StructField[] fields = new StructField[columns.size()];
        boolean arrayNative = config.getValue(DorisOptions.DORIS_READ_ARRAY_NATIVE_TYPE);
        for (int i = 0; i < columns.size(); i++) {
          Field column = columns.get(i);
          fields[i] =
              DataTypes.createStructField(
                  column.getName(),
                  SchemaConvertors.toCatalystType(
                      column.getType(), column.getPrecision(), column.getScale(), arrayNative),
                  true);
        }
        physical = new StructType(fields);
      } finally {
        frontend.close();
      }
      if (!schemasCompatible(schema, physical)) {
        return null;
      }
      Identifier readerIdentifier =
          "thrift".equalsIgnoreCase(nativeOptions.get("doris.read.mode"))
              ? identifier
              : quotedIdentifier(identifier);
      return new DorisTable(readerIdentifier, config, Option.apply(schema));
    } catch (Exception e) {
      // Third-party exception messages can contain catalog options or vended credentials.
      throw new IllegalArgumentException("Unable to initialize the Doris native reader");
    }
  }

  static boolean schemasCompatible(StructType logical, StructType physical) {
    StructField[] logicalFields = logical.fields();
    StructField[] physicalFields = physical.fields();
    if (logicalFields.length != physicalFields.length) {
      return false;
    }
    for (int i = 0; i < logicalFields.length; i++) {
      String name = logicalFields[i].name();
      // The Doris reader builds quoted SQL using the field name without escaping backticks, and
      // its predicate compiler renders multipart references differently from literal dots.
      if (name.contains("`")
          || name.contains(".")
          || !name.equals(physicalFields[i].name())
          || !logicalFields[i].dataType().equals(physicalFields[i].dataType())) {
        return false;
      }
    }
    return true;
  }

  static Identifier quotedIdentifier(Identifier identifier) {
    return new Identifier() {
      @Override
      public String[] namespace() {
        return identifier.namespace();
      }

      @Override
      public String name() {
        return identifier.name();
      }

      @Override
      public String toString() {
        return "`" + namespace()[0] + "`.`" + name() + "`";
      }
    };
  }
}
