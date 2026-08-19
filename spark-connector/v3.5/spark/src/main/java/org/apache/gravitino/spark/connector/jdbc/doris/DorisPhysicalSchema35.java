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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Immutable physical Doris schema snapshot for one authorized table read. */
final class DorisPhysicalSchema35 {

  private final StructType schema;
  private final List<String> dorisTypeNames;
  private final List<Boolean> catalystTypesResolved;
  private final List<Boolean> nullabilityKnown;

  DorisPhysicalSchema35(StructType schema, List<String> dorisTypeNames) {
    this(
        schema,
        dorisTypeNames,
        Collections.nCopies(schemaLength(schema), Boolean.TRUE),
        Collections.nCopies(schemaLength(schema), Boolean.TRUE));
  }

  DorisPhysicalSchema35(
      StructType schema,
      List<String> dorisTypeNames,
      List<Boolean> catalystTypesResolved,
      List<Boolean> nullabilityKnown) {
    Objects.requireNonNull(schema, "Doris physical schema must not be null");
    Objects.requireNonNull(dorisTypeNames, "Doris type names must not be null");
    Objects.requireNonNull(catalystTypesResolved, "Doris type resolution flags must not be null");
    Objects.requireNonNull(nullabilityKnown, "Doris nullability flags must not be null");
    if (schema.length() != dorisTypeNames.size()) {
      throw new IllegalArgumentException("Doris schema and type-name counts differ");
    }
    if (schema.length() != catalystTypesResolved.size()) {
      throw new IllegalArgumentException("Doris schema and type-resolution counts differ");
    }
    if (schema.length() != nullabilityKnown.size()) {
      throw new IllegalArgumentException("Doris schema and nullability counts differ");
    }
    this.schema = schema;
    this.dorisTypeNames = Collections.unmodifiableList(new ArrayList<>(dorisTypeNames));
    this.catalystTypesResolved =
        Collections.unmodifiableList(new ArrayList<>(catalystTypesResolved));
    this.nullabilityKnown = Collections.unmodifiableList(new ArrayList<>(nullabilityKnown));
  }

  /** Creates a Spark field whose nullable bit conservatively represents unknown Doris metadata. */
  static StructField createUnknownNullableField(String name, DataType dataType) {
    return DataTypes.createStructField(name, dataType, true);
  }

  StructType schema() {
    return schema;
  }

  String dorisTypeName(int index) {
    return dorisTypeNames.get(index);
  }

  boolean catalystTypeResolved(int index) {
    return catalystTypesResolved.get(index);
  }

  boolean nullabilityKnown(int index) {
    return nullabilityKnown.get(index);
  }

  private static int schemaLength(StructType schema) {
    return Objects.requireNonNull(schema, "Doris physical schema must not be null").length();
  }
}
