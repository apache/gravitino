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
package org.apache.gravitino.trino.connector.catalog.iceberg;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_EXPRESSION_ERROR;

import io.trino.spi.TrinoException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;

/** This class is used to convert expression of bucket, sort_by, partition object to string */
public class ExpressionUtil {
  private static final String IDENTIFIER = "[a-zA-Z_]\\w*";
  private static final String FUNCTION_ARG_INT = "(\\d+)";
  private static final String FUNCTION_ARG_IDENTIFIER = "(" + IDENTIFIER + ")";
  private static final Pattern YEAR_FUNCTION_PATTERN =
      Pattern.compile("year\\(" + FUNCTION_ARG_IDENTIFIER + "\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern MONTH_FUNCTION_PATTERN =
      Pattern.compile("month\\(" + FUNCTION_ARG_IDENTIFIER + "\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern DAY_FUNCTION_PATTERN =
      Pattern.compile("day\\(" + FUNCTION_ARG_IDENTIFIER + "\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern HOUR_FUNCTION_PATTERN =
      Pattern.compile("hour\\(" + FUNCTION_ARG_IDENTIFIER + "\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern BUCKET_FUNCTION_PATTERN =
      Pattern.compile(
          "bucket\\(" + FUNCTION_ARG_IDENTIFIER + ",\\s*" + FUNCTION_ARG_INT + "\\)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern TRUNCATE_FUNCTION_PATTERN =
      Pattern.compile(
          "truncate\\(" + FUNCTION_ARG_IDENTIFIER + ",\\s*" + FUNCTION_ARG_INT + "\\)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern IDENTIFILER_PATTERN =
      Pattern.compile(IDENTIFIER, Pattern.CASE_INSENSITIVE);

  private static final String SORT_DIRECTION_DESC = "DESC";
  private static final String NULL_ORDERING_FIRST = "FIRST";
  private static final String QUOTED_IDENTIFIER = "\"(?:\"\"|[^\"])*\"";
  // The sort field grammar of the Trino Iceberg connector: the direction and the null ordering are
  // both optional, and a column name outside the plain identifier form is double-quoted.
  private static final Pattern SORT_FIELD_PATTERN =
      Pattern.compile(
          "\\s*("
              + IDENTIFIER
              + "|"
              + QUOTED_IDENTIFIER
              + ")(?:\\s+(ASC|DESC))?(?:\\s+NULLS\\s+(FIRST|LAST))?\\s*",
          Pattern.CASE_INSENSITIVE);

  /**
   * Converts an array of transforms to a list of partition fields.
   *
   * @param transforms the array of transforms
   * @return the list of partition fields
   */
  public static List<String> expressionToPartitionFiled(Transform[] transforms) {
    try {
      List<String> partitionFields = new ArrayList<>();
      for (Transform transform : transforms) {
        partitionFields.add(transFormToString(transform));
      }
      return partitionFields;
    } catch (IllegalArgumentException e) {
      throw new TrinoException(
          GRAVITINO_EXPRESSION_ERROR,
          "Error to handle transform Expressions :" + e.getMessage(),
          e);
    }
  }

  /**
   * Converts a list of partition fields to an array of transforms.
   *
   * @param partitions the list of partition fields
   * @return the array of transforms
   */
  public static Transform[] partitionFiledToExpression(List<String> partitions) {
    try {
      List<Transform> partitionTransforms = new ArrayList<>();
      for (String partition : partitions) {
        parseTransform(partitionTransforms, partition);
      }
      return partitionTransforms.toArray(new Transform[0]);
    } catch (IllegalArgumentException e) {
      throw new TrinoException(
          GRAVITINO_EXPRESSION_ERROR, "Error parsing the partition field: " + e.getMessage(), e);
    }
  }

  /**
   * Converts an array of sort orders to a list of sort order fields.
   *
   * @param orders the array of sort orders
   * @return the list of sort order fields
   */
  public static List<String> expressionToSortOrderFiled(SortOrder[] orders) {
    try {
      List<String> orderFields = new ArrayList<>();
      for (SortOrder order : orders) {
        orderFields.add(sortOrderToString(order));
      }
      return orderFields;
    } catch (IllegalArgumentException e) {
      throw new TrinoException(
          GRAVITINO_EXPRESSION_ERROR,
          "Error to handle the sort order expressions : " + e.getMessage(),
          e);
    }
  }

  /**
   * Converts a list of sort order fields to an array of sort orders.
   *
   * @param orderFields the list of sort order fields
   * @return the array of sort orders
   */
  public static SortOrder[] sortOrderFiledToExpression(List<String> orderFields) {
    try {
      List<SortOrder> sortOrders = new ArrayList<>();
      for (String orderField : orderFields) {
        parseSortOrder(sortOrders, orderField);
      }
      return sortOrders.toArray(new SortOrder[0]);
    } catch (IllegalArgumentException e) {
      throw new TrinoException(
          GRAVITINO_EXPRESSION_ERROR, "Error parsing the sort order field: " + e.getMessage(), e);
    }
  }

  private static void parseTransform(List<Transform> transforms, String value) {
    boolean match =
        false
            || tryMatch(
                value,
                IDENTIFILER_PATTERN,
                (m) -> {
                  transforms.add(Transforms.identity(m.group(0)));
                })
            || tryMatch(
                value,
                YEAR_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(Transforms.year(m.group(1)));
                })
            || tryMatch(
                value,
                MONTH_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(Transforms.month(m.group(1)));
                })
            || tryMatch(
                value,
                DAY_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(Transforms.day(m.group(1)));
                })
            || tryMatch(
                value,
                HOUR_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(Transforms.hour(m.group(1)));
                })
            || tryMatch(
                value,
                BUCKET_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(
                      Transforms.bucket(Integer.parseInt(m.group(2)), new String[] {m.group(1)}));
                })
            || tryMatch(
                value,
                TRUNCATE_FUNCTION_PATTERN,
                (m) -> {
                  transforms.add(
                      Transforms.truncate(Integer.parseInt(m.group(2)), new String[] {m.group(1)}));
                });
    if (!match) {
      throw new IllegalArgumentException("Unparsed expression: " + value);
    }
  }

  private static boolean tryMatch(String value, Pattern pattern, MatchHandler handler) {
    Matcher matcher = pattern.matcher(value);
    if (matcher.matches()) {
      handler.invoke(matcher.toMatchResult());
      return true;
    }
    return false;
  }

  private static String transFormToString(Transform transform) {
    if (transform instanceof Transforms.IdentityTransform) {
      return ((Transforms.IdentityTransform) transform).fieldName()[0];

    } else if (transform instanceof Transforms.YearTransform
        || transform instanceof Transforms.MonthTransform
        || transform instanceof Transforms.DayTransform
        || transform instanceof Transforms.HourTransform) {
      return String.format(
          "%s(%s)", transform.name(), expressionToString(transform.arguments()[0]));

    } else if (transform instanceof Transforms.BucketTransform) {
      Transforms.BucketTransform bucketTransform = (Transforms.BucketTransform) transform;
      return String.format(
          "%s(%s, %s)",
          bucketTransform.name(), bucketTransform.fieldNames()[0][0], bucketTransform.numBuckets());

    } else if (transform instanceof Transforms.TruncateTransform) {
      Transforms.TruncateTransform truncateTransform = (Transforms.TruncateTransform) transform;
      return String.format(
          "%s(%s, %s)",
          truncateTransform.name(), truncateTransform.fieldName()[0], truncateTransform.width());
    }

    throw new IllegalArgumentException(
        String.format(
            "Unsupported transform %s with %d parameters: ",
            transform, transform.arguments().length));
  }

  private static String expressionToString(Expression expression) {
    if (expression instanceof NamedReference) {
      return ((NamedReference) expression).fieldName()[0];
    } else if (expression instanceof Literal<?>) {
      return ((Literal<?>) expression).value().toString();
    }
    throw new IllegalArgumentException("Unsupported expression: " + expression);
  }

  private static String sortOrderToString(SortOrder order) {
    Expression orderExpression = order.expression();
    if (!(orderExpression instanceof NamedReference)) {
      throw new IllegalArgumentException(
          "Only supported sort expression of NamedReference, the expression: " + orderExpression);
    }

    String columnName = columnToIdentifier(((NamedReference) orderExpression).fieldName()[0]);
    if (order.direction() == SortDirection.ASCENDING) {
      if (order.nullOrdering() == NullOrdering.NULLS_LAST) {
        return String.format("%s ASC NULLS LAST", columnName);
      } else {
        return columnName;
      }
    } else if (order.direction() == SortDirection.DESCENDING) {
      if (order.nullOrdering() == NullOrdering.NULLS_FIRST) {
        return String.format("%s DESC NULLS FIRST", columnName);
      } else {
        return columnName + " DESC";
      }
    }
    throw new IllegalArgumentException("Unsupported sort order: " + order);
  }

  private static void parseSortOrder(List<SortOrder> sortOrders, String value) {
    boolean match =
        tryMatch(
            value,
            SORT_FIELD_PATTERN,
            (m) -> {
              NamedReference.FieldReference sortField =
                  NamedReference.field(identifierToColumn(m.group(1)));
              SortDirection sortDirection =
                  SORT_DIRECTION_DESC.equalsIgnoreCase(m.group(2))
                      ? SortDirection.DESCENDING
                      : SortDirection.ASCENDING;
              if (m.group(3) == null) {
                sortOrders.add(SortOrders.of(sortField, sortDirection));
              } else {
                NullOrdering nullOrdering =
                    NULL_ORDERING_FIRST.equalsIgnoreCase(m.group(3))
                        ? NullOrdering.NULLS_FIRST
                        : NullOrdering.NULLS_LAST;
                sortOrders.add(SortOrders.of(sortField, sortDirection, nullOrdering));
              }
            });
    if (!match) {
      throw new IllegalArgumentException("Unparsed expression: " + value);
    }
  }

  private static String identifierToColumn(String identifier) {
    if (identifier.startsWith("\"")) {
      return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
    }
    return identifier;
  }

  private static String columnToIdentifier(String column) {
    if (IDENTIFILER_PATTERN.matcher(column).matches()) {
      return column;
    }
    return "\"" + column.replace("\"", "\"\"") + "\"";
  }

  interface MatchHandler {
    void invoke(MatchResult matchResult);
  }
}
