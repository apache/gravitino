/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.NotSupportedException;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.connector.expressions.ApplyTransform;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.DaysTransform;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.HoursTransform;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.MonthsTransform;
import org.apache.spark.sql.connector.expressions.SortedBucketTransform;
import org.apache.spark.sql.connector.expressions.YearsTransform;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import scala.collection.JavaConverters;

/**
 * SparkTransformConverter translate between Spark transform and Gravitino partition, distribution,
 * sort orders. There may be multi partition transforms, but should be only one bucket transform.
 *
 * <p>Spark bucket transform is corresponding to Gravitino Hash distribution without sort orders.
 *
 * <p>Spark sorted bucket transform is corresponding to Gravitino Hash distribution with sort
 * orders.
 */
public class SparkTransformConverter {

  @Getter
  public static class DistributionAndSortOrdersInfo {
    private Distribution distribution;
    private SortOrder[] sortOrders;

    private void setDistribution(Distribution distributionInfo) {
      Preconditions.checkState(distribution == null, "Should only set distribution once");
      this.distribution = distributionInfo;
    }

    private void setSortOrders(SortOrder[] sortOrdersInfo) {
      Preconditions.checkState(sortOrders == null, "Should only set sort orders once");
      this.sortOrders = sortOrdersInfo;
    }
  }

  public static Transform[] toGravitinoPartitionings(
      org.apache.spark.sql.connector.expressions.Transform[] transforms, boolean isHiveProvider) {
    if (ArrayUtils.isEmpty(transforms)) {
      return Transforms.EMPTY_TRANSFORM;
    }

    return Arrays.stream(transforms)
        .filter(transform -> !isSortBucketTransform(transform))
        .map(
            transform -> {
              if (transform instanceof BucketTransform) {
                if (isHiveProvider) {
                  return Transforms.EMPTY_TRANSFORM;
                }
                BucketTransform bucketTransform = (BucketTransform) transform;
                int numBuckets = (int) bucketTransform.numBuckets().value();
                String[][] fieldNames =
                    Arrays.stream(bucketTransform.references())
                        .map(org.apache.spark.sql.connector.expressions.NamedReference::fieldNames)
                        .toArray(String[][]::new);
                return Transforms.bucket(numBuckets, fieldNames);
              } else if (transform instanceof IdentityTransform) {
                IdentityTransform identityTransform = (IdentityTransform) transform;
                return Transforms.identity(identityTransform.reference().fieldNames());
              } else if (transform instanceof HoursTransform) {
                HoursTransform hoursTransform = (HoursTransform) transform;
                return Transforms.hour(hoursTransform.reference().fieldNames());
              } else if (transform instanceof DaysTransform) {
                DaysTransform daysTransform = (DaysTransform) transform;
                return Transforms.day(daysTransform.reference().fieldNames());
              } else if (transform instanceof MonthsTransform) {
                MonthsTransform monthsTransform = (MonthsTransform) transform;
                return Transforms.month(monthsTransform.reference().fieldNames());
              } else if (transform instanceof YearsTransform) {
                YearsTransform yearsTransform = (YearsTransform) transform;
                return Transforms.year(yearsTransform.reference().fieldNames());
              } else if (transform instanceof ApplyTransform
                  && "truncate".equals(transform.name())) {
                return Transforms.truncate(
                    findWidth(transform),
                    String.join(ConnectorConstants.DOT, transform.references()[0].fieldNames()));
              } else {
                throw new NotSupportedException(
                    "Doesn't support Spark transform: " + transform.name());
              }
            })
        .toArray(Transform[]::new);
  }

  public static DistributionAndSortOrdersInfo toGravitinoDistributionAndSortOrders(
      org.apache.spark.sql.connector.expressions.Transform[] transforms) {
    DistributionAndSortOrdersInfo distributionAndSortOrdersInfo =
        new DistributionAndSortOrdersInfo();
    if (ArrayUtils.isEmpty(transforms)) {
      return distributionAndSortOrdersInfo;
    }

    Arrays.stream(transforms)
        .filter(transform -> isBucketTransform(transform))
        .forEach(
            transform -> {
              if (transform instanceof SortedBucketTransform) {
                Pair<Distribution, SortOrder[]> pair =
                    toGravitinoDistributionAndSortOrders((SortedBucketTransform) transform);
                distributionAndSortOrdersInfo.setDistribution(pair.getLeft());
                distributionAndSortOrdersInfo.setSortOrders(pair.getRight());
              } else if (transform instanceof BucketTransform) {
                BucketTransform bucketTransform = (BucketTransform) transform;
                Distribution distribution = toGravitinoDistribution(bucketTransform);
                distributionAndSortOrdersInfo.setDistribution(distribution);
              } else {
                throw new NotSupportedException(
                    "Only support BucketTransform and SortedBucketTransform, but get: "
                        + transform.name());
              }
            });

    return distributionAndSortOrdersInfo;
  }

  public static org.apache.spark.sql.connector.expressions.Transform[] toSparkTransform(
      com.datastrato.gravitino.rel.expressions.transforms.Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrder) {
    List<org.apache.spark.sql.connector.expressions.Transform> sparkTransforms = new ArrayList<>();
    if (ArrayUtils.isNotEmpty(partitions)) {
      Arrays.stream(partitions)
          .forEach(
              transform -> {
                if (transform instanceof Transforms.IdentityTransform) {
                  Transforms.IdentityTransform identityTransform =
                      (Transforms.IdentityTransform) transform;
                  sparkTransforms.add(
                      createSparkIdentityTransform(
                          String.join(ConnectorConstants.DOT, identityTransform.fieldName())));
                } else if (transform instanceof Transforms.HourTransform) {
                  Transforms.HourTransform hourTransform = (Transforms.HourTransform) transform;
                  sparkTransforms.add(createSparkHoursTransform(hourTransform.fieldName()));
                } else if (transform instanceof Transforms.BucketTransform) {
                  Transforms.BucketTransform bucketTransform =
                      (Transforms.BucketTransform) transform;
                  int numBuckets = bucketTransform.numBuckets();
                  String[] fieldNames =
                      Arrays.stream(bucketTransform.fieldNames())
                          .map(f -> String.join(ConnectorConstants.DOT, f))
                          .toArray(String[]::new);
                  sparkTransforms.add(createSparkBucketTransform(numBuckets, fieldNames));
                } else if (transform instanceof Transforms.DayTransform) {
                  Transforms.DayTransform dayTransform = (Transforms.DayTransform) transform;
                  sparkTransforms.add(createSparkDaysTransform(dayTransform.fieldName()));
                } else if (transform instanceof Transforms.MonthTransform) {
                  Transforms.MonthTransform monthTransform = (Transforms.MonthTransform) transform;
                  sparkTransforms.add(createSparkMonthsTransform(monthTransform.fieldName()));
                } else if (transform instanceof Transforms.YearTransform) {
                  Transforms.YearTransform yearTransform = (Transforms.YearTransform) transform;
                  sparkTransforms.add(createSparkYearsTransform(yearTransform.fieldName()));
                } else if (transform instanceof Transforms.TruncateTransform) {
                  Transforms.TruncateTransform truncateTransform =
                      (Transforms.TruncateTransform) transform;
                  int width = truncateTransform.width();
                  String[] fieldName = truncateTransform.fieldName();
                  sparkTransforms.add(createSparkTruncateTransform("truncate", width, fieldName));
                } else {
                  throw new UnsupportedOperationException(
                      "Doesn't support Gravitino partition: "
                          + transform.name()
                          + ", className: "
                          + transform.getClass().getName());
                }
              });
    }

    org.apache.spark.sql.connector.expressions.Transform bucketTransform =
        toSparkBucketTransform(distribution, sortOrder);
    if (bucketTransform != null) {
      sparkTransforms.add(bucketTransform);
    }

    return sparkTransforms.toArray(new org.apache.spark.sql.connector.expressions.Transform[0]);
  }

  private static Distribution toGravitinoDistribution(BucketTransform bucketTransform) {
    int bucketNum = (Integer) bucketTransform.numBuckets().value();
    Expression[] expressions =
        JavaConverters.seqAsJavaList(bucketTransform.columns()).stream()
            .map(sparkReference -> NamedReference.field(sparkReference.fieldNames()))
            .toArray(Expression[]::new);
    return Distributions.hash(bucketNum, expressions);
  }

  // Spark datasourceV2 doesn't support specify sort order direction, use ASCENDING as default.
  private static Pair<Distribution, SortOrder[]> toGravitinoDistributionAndSortOrders(
      SortedBucketTransform sortedBucketTransform) {
    int bucketNum = (Integer) sortedBucketTransform.numBuckets().value();
    Expression[] bucketColumns =
        toGravitinoNamedReference(JavaConverters.seqAsJavaList(sortedBucketTransform.columns()));

    Expression[] sortColumns =
        toGravitinoNamedReference(
            JavaConverters.seqAsJavaList(sortedBucketTransform.sortedColumns()));
    SortOrder[] sortOrders =
        Arrays.stream(sortColumns)
            .map(
                sortColumn ->
                    SortOrders.of(sortColumn, ConnectorConstants.SPARK_DEFAULT_SORT_DIRECTION))
            .toArray(SortOrder[]::new);

    return Pair.of(Distributions.hash(bucketNum, bucketColumns), sortOrders);
  }

  private static org.apache.spark.sql.connector.expressions.Transform toSparkBucketTransform(
      Distribution distribution, SortOrder[] sortOrders) {
    if (distribution == null) {
      return null;
    }

    switch (distribution.strategy()) {
      case NONE:
        return null;
      case HASH:
        int bucketNum = distribution.number();
        String[] bucketFields =
            Arrays.stream(distribution.expressions())
                .map(
                    expression ->
                        getFieldNameFromGravitinoNamedReference((NamedReference) expression))
                .toArray(String[]::new);
        if (sortOrders == null || sortOrders.length == 0) {
          return Expressions.bucket(bucketNum, bucketFields);
        } else {
          String[] sortOrderFields =
              Arrays.stream(sortOrders)
                  .map(
                      sortOrder ->
                          getFieldNameFromGravitinoNamedReference(
                              (NamedReference) sortOrder.expression()))
                  .toArray(String[]::new);
          return createSortBucketTransform(bucketNum, bucketFields, sortOrderFields);
        }
        // Spark doesn't support EVEN or RANGE distribution
      default:
        throw new NotSupportedException(
            "Doesn't support distribution strategy: " + distribution.strategy());
    }
  }

  private static Expression[] toGravitinoNamedReference(
      List<org.apache.spark.sql.connector.expressions.NamedReference> sparkNamedReferences) {
    return sparkNamedReferences.stream()
        .map(sparkReference -> NamedReference.field(sparkReference.fieldNames()))
        .toArray(Expression[]::new);
  }

  public static org.apache.spark.sql.connector.expressions.Transform createSortBucketTransform(
      int bucketNum, String[] bucketFields, String[] sortFields) {
    return LogicalExpressions.bucket(
        bucketNum, createSparkNamedReference(bucketFields), createSparkNamedReference(sortFields));
  }

  // columnName could be "a" or "a.b" for nested column
  public static IdentityTransform createSparkIdentityTransform(String columnName) {
    return IdentityTransform.apply(Expressions.column(columnName));
  }

  public static HoursTransform createSparkHoursTransform(String[] fieldName) {
    return LogicalExpressions.hours(
        Expressions.column(String.join(ConnectorConstants.DOT, fieldName)));
  }

  public static BucketTransform createSparkBucketTransform(int numBuckets, String[] fieldNames) {
    return LogicalExpressions.bucket(numBuckets, createSparkNamedReference(fieldNames));
  }

  public static DaysTransform createSparkDaysTransform(String[] fieldName) {
    return LogicalExpressions.days(
        Expressions.column(String.join(ConnectorConstants.DOT, fieldName)));
  }

  public static MonthsTransform createSparkMonthsTransform(String[] fieldName) {
    return LogicalExpressions.months(
        Expressions.column(String.join(ConnectorConstants.DOT, fieldName)));
  }

  public static YearsTransform createSparkYearsTransform(String[] FieldName) {
    return LogicalExpressions.years(
        Expressions.column(String.join(ConnectorConstants.DOT, FieldName)));
  }

  public static org.apache.spark.sql.connector.expressions.Transform createSparkTruncateTransform(
      String functionName, int width, String[] fieldName) {
    return Expressions.apply(
        functionName,
        Expressions.literal(width),
        Expressions.column(String.join(ConnectorConstants.DOT, fieldName)));
  }

  private static org.apache.spark.sql.connector.expressions.NamedReference[]
      createSparkNamedReference(String[] fields) {
    return Arrays.stream(fields)
        .map(Expressions::column)
        .toArray(org.apache.spark.sql.connector.expressions.NamedReference[]::new);
  }

  // Gravitino use ["a","b"] for nested fields while Spark use "a.b";
  private static String getFieldNameFromGravitinoNamedReference(
      NamedReference gravitinoNamedReference) {
    return String.join(ConnectorConstants.DOT, gravitinoNamedReference.fieldName());
  }

  private static boolean isBucketTransform(
      org.apache.spark.sql.connector.expressions.Transform transform) {
    return transform instanceof BucketTransform || transform instanceof SortedBucketTransform;
  }

  private static boolean isSortBucketTransform(
      org.apache.spark.sql.connector.expressions.Transform transform) {
    return transform instanceof SortedBucketTransform;
  }

  // Referred from org.apache.iceberg.spark.Spark3Util
  private static int findWidth(org.apache.spark.sql.connector.expressions.Transform transform) {
    for (org.apache.spark.sql.connector.expressions.Expression expr : transform.arguments()) {
      if (expr instanceof Literal) {
        if (((Literal) expr).dataType() instanceof IntegerType) {
          Literal<Integer> lit = (Literal<Integer>) expr;
          Preconditions.checkArgument(
              lit.value() > 0, "Unsupported width for transform: %s", transform.describe());
          return lit.value();

        } else if (((Literal) expr).dataType() instanceof LongType) {
          Literal<Long> lit = (Literal<Long>) expr;
          Preconditions.checkArgument(
              lit.value() > 0 && lit.value() < Integer.MAX_VALUE,
              "Unsupported width for transform: %s",
              transform.describe());
          if (lit.value() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
          }
          return lit.value().intValue();
        }
      }
    }

    throw new IllegalArgumentException("Cannot find width for transform: " + transform.describe());
  }
}
