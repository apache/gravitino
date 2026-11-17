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

import javax.annotation.Nullable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Column lookup shared by JDBC schema alignment and pushdown checks. */
final class JdbcSchemaUtils {

  private JdbcSchemaUtils() {}

  @Nullable
  static DataType fieldType(StructType schema, String name) {
    DataType candidate = null;
    boolean ambiguous = false;
    for (StructField field : schema.fields()) {
      // PostgreSQL can have distinct quoted columns that differ only in case.
      if (field.name().equals(name)) {
        return field.dataType();
      }
      if (field.name().equalsIgnoreCase(name)) {
        ambiguous |= candidate != null;
        candidate = field.dataType();
      }
    }
    // Preserve case-insensitive lookup only when it identifies a single source column.
    return ambiguous ? null : candidate;
  }
}
