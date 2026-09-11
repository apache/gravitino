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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.StructType;

/** Validated Spark-visible Doris schema and its SQL projection plan. */
final class DorisReadSchema35 {

  private final StructType schema;
  private final List<String> projections;
  private final boolean requiresSqlExecution;
  private final Map<String, String> normalizedTypeNames;

  DorisReadSchema35(
      StructType schema,
      List<String> projections,
      boolean requiresSqlExecution,
      Map<String, String> normalizedTypeNames) {
    if (schema.length() != projections.size()) {
      throw new IllegalArgumentException("Doris schema and projection counts differ");
    }
    this.schema = schema;
    this.projections = ImmutableList.copyOf(projections);
    this.requiresSqlExecution = requiresSqlExecution;
    this.normalizedTypeNames = ImmutableMap.copyOf(normalizedTypeNames);
  }

  StructType schema() {
    return schema;
  }

  List<String> projections() {
    return projections;
  }

  boolean requiresSqlExecution() {
    return requiresSqlExecution;
  }

  Set<String> normalizedColumns() {
    return ImmutableSet.copyOf(normalizedTypeNames.keySet());
  }

  String normalizedTypeName(String column) {
    return normalizedTypeNames.get(column);
  }

  String tableOrQuery(Identifier identifier) {
    if (identifier.namespace().length != 1) {
      throw new IllegalArgumentException("Doris table identifiers require one schema");
    }
    if (projections.isEmpty()) {
      throw new IllegalArgumentException("Doris JDBC reads require at least one projected column");
    }
    return String.format(
        "(SELECT %s FROM %s.%s) gravitino_doris_source",
        String.join(", ", projections),
        quoteIdentifier(identifier.namespace()[0]),
        quoteIdentifier(identifier.name()));
  }

  static String quoteIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }
}
