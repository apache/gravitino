/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.Literal;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import java.util.Locale;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;

/** Implement gravitino sort order converter to iceberg sort order. */
public class ToIcebergSortOrder {

  private static final String DOT = ".";

  /**
   * Convert Gravitino order to Iceberg.
   *
   * @param schema Iceberg schema.
   * @param sortOrders Gravitino sort order.
   * @return Iceberg sort order.
   */
  public static org.apache.iceberg.SortOrder toSortOrder(Schema schema, SortOrder[] sortOrders) {
    if (ArrayUtils.isEmpty(sortOrders)) {
      return null;
    }
    org.apache.iceberg.SortOrder.Builder sortOrderBuilder =
        org.apache.iceberg.SortOrder.builderFor(schema);
    for (SortOrder sortOrder : sortOrders) {
      Expression expression = sortOrder.expression();
      if (expression instanceof NamedReference.FieldReference) {
        String fieldName =
            String.join(DOT, ((NamedReference.FieldReference) expression).fieldName());
        sortOrderBuilder.sortBy(
            fieldName, toIceberg(sortOrder.direction()), toIceberg(sortOrder.nullOrdering()));
      } else if (expression instanceof FunctionExpression) {
        FunctionExpression gravitinoExpr = (FunctionExpression) expression;
        String colName;
        if (gravitinoExpr.arguments().length == 1) {
          colName =
              String.join(
                  DOT, ((NamedReference.FieldReference) gravitinoExpr.arguments()[0]).fieldName());
        } else if (gravitinoExpr.arguments().length == 2) {
          colName =
              String.join(
                  DOT, ((NamedReference.FieldReference) gravitinoExpr.arguments()[1]).fieldName());
        } else {
          throw new UnsupportedOperationException(
              "Sort function is not supported: " + gravitinoExpr.functionName());
        }

        UnboundTerm<Object> icebergExpression;
        switch (gravitinoExpr.functionName().toLowerCase(Locale.ROOT)) {
          case "bucket":
            int numBuckets = ((Literal<Integer>) gravitinoExpr.arguments()[0]).value();
            icebergExpression = Expressions.bucket(colName, numBuckets);
            break;
          case "truncate":
            int width = ((Literal<Integer>) gravitinoExpr.arguments()[0]).value();
            icebergExpression = Expressions.truncate(colName, width);
            break;
          case "year":
            icebergExpression = Expressions.year(colName);
            break;
          case "month":
            icebergExpression = Expressions.month(colName);
            break;
          case "day":
            icebergExpression = Expressions.day(colName);
            break;
          case "hour":
            icebergExpression = Expressions.hour(colName);
            break;
          default:
            throw new UnsupportedOperationException(
                "Sort function is not supported: " + gravitinoExpr.functionName());
        }
        sortOrderBuilder.sortBy(
            icebergExpression,
            toIceberg(sortOrder.direction()),
            toIceberg(sortOrder.nullOrdering()));
      } else {
        throw new UnsupportedOperationException("Sort expression is not supported: " + expression);
      }
    }
    return sortOrderBuilder.build();
  }

  private static NullOrder toIceberg(NullOrdering nullOrdering) {
    return nullOrdering == NullOrdering.NULLS_FIRST ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
  }

  private static org.apache.iceberg.SortDirection toIceberg(SortDirection direction) {
    return direction == SortDirection.ASCENDING
        ? org.apache.iceberg.SortDirection.ASC
        : org.apache.iceberg.SortDirection.DESC;
  }
}
