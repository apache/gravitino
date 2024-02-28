/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.transforms.SortOrderVisitor;

/**
 * Implement iceberg sort order converter to gravitino sort order.
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
    return functionSortOrder("bucket", id, direction, nullOrder);
  }

  @Override
  public SortOrder truncate(
      String sourceName, int id, int width, SortDirection direction, NullOrder nullOrder) {
    return functionSortOrder("truncate", id, direction, nullOrder);
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

  private com.datastrato.gravitino.rel.expressions.sorts.SortDirection toGravitino(
      SortDirection direction) {
    return direction == SortDirection.ASC
        ? com.datastrato.gravitino.rel.expressions.sorts.SortDirection.ASCENDING
        : com.datastrato.gravitino.rel.expressions.sorts.SortDirection.DESCENDING;
  }

  private NullOrdering toGravitino(NullOrder nullOrder) {
    return nullOrder == NullOrder.NULLS_FIRST ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST;
  }

  /**
   * Convert Iceberg order to Gravitino.
   *
   * @param sortOrder
   * @return Gravitino sort order
   */
  public static SortOrder[] fromSortOrder(org.apache.iceberg.SortOrder sortOrder) {
    FromIcebergSortOrder visitor = new FromIcebergSortOrder(sortOrder.schema());
    List<SortOrder> ordering = SortOrderVisitor.visit(sortOrder, visitor);
    return ordering.toArray(new SortOrder[0]);
  }
}
