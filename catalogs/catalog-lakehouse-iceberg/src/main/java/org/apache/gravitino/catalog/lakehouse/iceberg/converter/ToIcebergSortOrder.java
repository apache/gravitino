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

import com.google.common.base.Preconditions;
import java.util.Locale;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;

/** Implement Apache Gravitino sort order converter to Apache Iceberg sort order. */
public class ToIcebergSortOrder {

  private ToIcebergSortOrder() {}

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
      if (sortOrder.expression() instanceof NamedReference.FieldReference) {
        String fieldName =
            String.join(DOT, ((NamedReference.FieldReference) sortOrder.expression()).fieldName());
        sortOrderBuilder.sortBy(
            fieldName, toIceberg(sortOrder.direction()), toIceberg(sortOrder.nullOrdering()));
        continue;
      }

      if (sortOrder.expression() instanceof FunctionExpression) {
        FunctionExpression sortFunc = (FunctionExpression) sortOrder.expression();
        UnboundTerm<Object> icebergExpression;
        switch (sortFunc.functionName().toLowerCase(Locale.ROOT)) {
          case "bucket":
            Preconditions.checkArgument(
                sortFunc.arguments().length == 2, "Bucket sort should have 2 arguments");

            Expression firstArg = sortFunc.arguments()[0];
            Preconditions.checkArgument(
                firstArg instanceof Literal
                    && ((Literal<?>) firstArg).dataType() instanceof Types.IntegerType,
                "Bucket sort's first argument must be a integer literal");
            int numBuckets = Integer.parseInt(String.valueOf(((Literal<?>) firstArg).value()));

            Expression secondArg = sortFunc.arguments()[1];
            Preconditions.checkArgument(
                secondArg instanceof NamedReference.FieldReference,
                "Bucket sort's second argument must be a field reference");
            String fieldName =
                String.join(DOT, ((NamedReference.FieldReference) secondArg).fieldName());

            icebergExpression = Expressions.bucket(fieldName, numBuckets);
            break;
          case "truncate":
            Preconditions.checkArgument(
                sortFunc.arguments().length == 2, "Truncate sort should have 2 arguments");

            firstArg = sortFunc.arguments()[0];
            Preconditions.checkArgument(
                firstArg instanceof Literal
                    && ((Literal<?>) firstArg).dataType() instanceof Types.IntegerType,
                "Truncate sort's first argument must be a integer literal");
            int width = Integer.parseInt(String.valueOf(((Literal<?>) firstArg).value()));

            secondArg = sortFunc.arguments()[1];
            Preconditions.checkArgument(
                secondArg instanceof NamedReference.FieldReference,
                "Truncate sort's second argument must be a field reference");
            fieldName = String.join(DOT, ((NamedReference.FieldReference) secondArg).fieldName());

            icebergExpression = Expressions.truncate(fieldName, width);
            break;
          case "year":
            icebergExpression = Expressions.year(getValidSingleField("year", sortFunc.arguments()));
            break;
          case "month":
            icebergExpression =
                Expressions.month(getValidSingleField("month", sortFunc.arguments()));
            break;
          case "day":
            icebergExpression = Expressions.day(getValidSingleField("day", sortFunc.arguments()));
            break;
          case "hour":
            icebergExpression = Expressions.hour(getValidSingleField("hour", sortFunc.arguments()));
            break;
          default:
            throw new UnsupportedOperationException(
                "Sort function is not supported: " + sortFunc.functionName());
        }
        sortOrderBuilder.sortBy(
            icebergExpression,
            toIceberg(sortOrder.direction()),
            toIceberg(sortOrder.nullOrdering()));
        continue;
      }

      throw new UnsupportedOperationException(
          "Sort expression is not supported: " + sortOrder.expression());
    }
    return sortOrderBuilder.build();
  }

  private static String getValidSingleField(String functionName, Expression[] arguments) {
    Preconditions.checkArgument(
        arguments.length == 1,
        "Sort function %s should have 1 argument, but got %s",
        functionName,
        arguments.length);
    Expression argument = arguments[0];
    Preconditions.checkArgument(
        argument instanceof NamedReference.FieldReference,
        "Sort function %s's argument should be a field reference, but got %s",
        functionName,
        argument);
    NamedReference.FieldReference fieldReference = (NamedReference.FieldReference) argument;
    return String.join(DOT, fieldReference.fieldName());
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
