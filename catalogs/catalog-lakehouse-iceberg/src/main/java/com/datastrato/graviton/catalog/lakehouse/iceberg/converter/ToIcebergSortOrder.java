/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_BUCKET;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_TRUNCATE;

import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.base.Preconditions;
import io.substrait.expression.Expression;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;

/** Implement iceberg sort order converter to graviton sort order. */
public class ToIcebergSortOrder {

  private static final String DOT = ".";

  /**
   * Convert Graviton order to Iceberg.
   *
   * @param schema Iceberg schema.
   * @param sortOrders Graviton sort order.
   * @return Iceberg sort order.
   */
  public static org.apache.iceberg.SortOrder toSortOrder(Schema schema, SortOrder[] sortOrders) {
    if (ArrayUtils.isEmpty(sortOrders)) {
      return null;
    }
    org.apache.iceberg.SortOrder.Builder sortOrderBuilder =
        org.apache.iceberg.SortOrder.builderFor(schema);
    for (SortOrder sortOrder : sortOrders) {
      Transform transform = sortOrder.getTransform();
      if (transform instanceof Transforms.NamedReference) {
        String fieldName = String.join(DOT, ((Transforms.NamedReference) transform).value());
        sortOrderBuilder.sortBy(
            fieldName, toIceberg(sortOrder.getDirection()), toIceberg(sortOrder.getNullOrdering()));
      } else if (transform instanceof Transforms.FunctionTrans) {
        Preconditions.checkArgument(
            transform.arguments().length == 1,
            "Iceberg sort order does not support nested field",
            transform);
        String colName =
            Arrays.stream(transform.arguments())
                .map(t -> ((Transforms.NamedReference) t).value()[0])
                .collect(Collectors.joining(DOT));
        UnboundTerm<Object> expression;
        switch (transform.name().toLowerCase(Locale.ROOT)) {
          case NAME_OF_BUCKET:
            int numBuckets =
                ((Expression.I32Literal)
                        ((Transforms.LiteralReference) transform.arguments()[0]).value())
                    .value();
            expression = Expressions.bucket(colName, numBuckets);
            break;
          case NAME_OF_TRUNCATE:
            int width =
                ((Expression.I32Literal)
                        ((Transforms.LiteralReference) transform.arguments()[0]).value())
                    .value();
            expression = Expressions.truncate(colName, width);
            break;
          case Transforms.NAME_OF_YEAR:
            expression = Expressions.year(colName);
            break;
          case Transforms.NAME_OF_MONTH:
            expression = Expressions.month(colName);
            break;
          case Transforms.NAME_OF_DAY:
            expression = Expressions.day(colName);
            break;
          case Transforms.NAME_OF_HOUR:
            expression = Expressions.hour(colName);
            break;
          default:
            throw new UnsupportedOperationException(
                "Transform is not supported: " + transform.name());
        }
        sortOrderBuilder.sortBy(
            expression,
            toIceberg(sortOrder.getDirection()),
            toIceberg(sortOrder.getNullOrdering()));
      } else {
        throw new UnsupportedOperationException("Transform is not supported: " + transform.name());
      }
    }
    return sortOrderBuilder.build();
  }

  private static NullOrder toIceberg(SortOrder.NullOrdering nullOrdering) {
    return nullOrdering == SortOrder.NullOrdering.FIRST
        ? NullOrder.NULLS_FIRST
        : NullOrder.NULLS_LAST;
  }

  private static SortDirection toIceberg(SortOrder.Direction direction) {
    return direction == SortOrder.Direction.ASC ? SortDirection.ASC : SortDirection.DESC;
  }
}
