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
package org.apache.gravitino.catalog.lakehouse.iceberg.converter;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.transforms.SortOrderVisitor;

/**
 * Implement Apache Iceberg sort order converter to Apache Gravitino sort order.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/SortOrderToSpark.java
 */
public class FromIcebergSortOrder implements SortOrderVisitor<SortOrder> {

  private final Map<Integer, String> idToName;

  public FromIcebergSortOrder(Schema schema) {
    this.idToName = schema.idToName();
  }

  @Override
  public SortOrder field(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return fieldSortOrder(id, direction, nullOrder);
  }

  @Override
  public SortOrder bucket(
      String sourceName, int id, int width, SortDirection direction, NullOrder nullOrder) {
    return functionSortOrder("bucket", width, id, direction, nullOrder);
  }

  @Override
  public SortOrder truncate(
      String sourceName, int id, int width, SortDirection direction, NullOrder nullOrder) {
    return functionSortOrder("truncate", width, id, direction, nullOrder);
  }

  @Override
  public SortOrder year(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return functionSortOrder(Transforms.NAME_OF_YEAR, id, direction, nullOrder);
  }

  @Override
  public SortOrder month(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return functionSortOrder(Transforms.NAME_OF_MONTH, id, direction, nullOrder);
  }

  @Override
  public SortOrder day(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return functionSortOrder(Transforms.NAME_OF_DAY, id, direction, nullOrder);
  }

  @Override
  public SortOrder hour(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return functionSortOrder(Transforms.NAME_OF_HOUR, id, direction, nullOrder);
  }

  private SortOrder fieldSortOrder(int id, SortDirection direction, NullOrder nullOrder) {
    return SortOrders.of(
        NamedReference.field(idToName.get(id)), toGravitino(direction), toGravitino(nullOrder));
  }

  private SortOrder functionSortOrder(
      String name, int id, SortDirection direction, NullOrder nullOrder) {
    return SortOrders.of(
        FunctionExpression.of(name, NamedReference.field(idToName.get(id))),
        toGravitino(direction),
        toGravitino(nullOrder));
  }

  private SortOrder functionSortOrder(
      String name, int width, int id, SortDirection direction, NullOrder nullOrder) {
    return SortOrders.of(
        FunctionExpression.of(
            name, Literals.integerLiteral(width), NamedReference.field(idToName.get(id))),
        toGravitino(direction),
        toGravitino(nullOrder));
  }

  private org.apache.gravitino.rel.expressions.sorts.SortDirection toGravitino(
      SortDirection direction) {
    return direction == SortDirection.ASC
        ? org.apache.gravitino.rel.expressions.sorts.SortDirection.ASCENDING
        : org.apache.gravitino.rel.expressions.sorts.SortDirection.DESCENDING;
  }

  private NullOrdering toGravitino(NullOrder nullOrder) {
    return nullOrder == NullOrder.NULLS_FIRST ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST;
  }

  /**
   * Convert Iceberg order to Gravitino.
   *
   * @param sortOrder Iceberg sort order
   * @return Gravitino sort order
   */
  public static SortOrder[] fromSortOrder(org.apache.iceberg.SortOrder sortOrder) {
    FromIcebergSortOrder visitor = new FromIcebergSortOrder(sortOrder.schema());
    List<SortOrder> ordering = SortOrderVisitor.visit(sortOrder, visitor);
    return ordering.toArray(new SortOrder[0]);
  }
}
