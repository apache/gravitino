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

package org.apache.gravitino.maintenance.optimizer.recommender.util;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.rel.Table;

/**
 * Utility for translating table metadata into variables usable in a {@code trigger-expr} or {@code
 * score-expr} expression.
 */
public class TableMetadataTriggerExpressionUtils {

  private TableMetadataTriggerExpressionUtils() {}

  /**
   * Builds a context map for evaluating table metadata trigger expressions.
   *
   * <p>This method creates a map containing:
   *
   * <ul>
   *   <li>Built-in table metadata: {@code column_count}, {@code partition_count}, {@code
   *       sort_order_count}
   *   <li>All table properties from {@link Table#properties()}, with numeric strings converted to
   *       {@code long} values and all others kept as {@code String} values
   * </ul>
   *
   * @param tableMetadata the table metadata to extract properties from
   * @return a context map suitable for expression evaluation
   */
  public static Map<String, Object> buildTableMetadataContext(Table tableMetadata) {
    Preconditions.checkArgument(tableMetadata != null, "Table metadata is null");
    Map<String, Object> context = new HashMap<>();
    context.put(
        TableMetadataTriggerExpression.COLUMN_COUNT.getName(),
        tableMetadata.columns() != null ? tableMetadata.columns().length : 0);
    context.put(
        TableMetadataTriggerExpression.PARTITION_COUNT.getName(),
        tableMetadata.partitioning() != null ? tableMetadata.partitioning().length : 0);
    context.put(
        TableMetadataTriggerExpression.SORT_ORDER_COUNT.getName(),
        tableMetadata.sortOrder() != null ? tableMetadata.sortOrder().length : 0);

    if (tableMetadata.properties() != null) {
      tableMetadata
          .properties()
          .forEach(
              (k, v) -> {
                try {
                  context.put(k, Long.parseLong(v));
                } catch (NumberFormatException e) {
                  // For non-numeric properties, we just put the value as is.
                  context.put(k, v);
                }
              });
    }
    return context;
  }

  /** Supported built-in table metadata trigger expression variables. */
  private enum TableMetadataTriggerExpression {
    COLUMN_COUNT,
    PARTITION_COUNT,
    SORT_ORDER_COUNT;

    public String getName() {
      return name().toLowerCase();
    }
  }
}
