/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.dto.rel.partitioning.Partitioning.SingleFieldPartitioning;
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
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.SortedBucketTransform;
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

  public static Transform[] toGravitinoPartitions(
      org.apache.spark.sql.connector.expressions.Transform[] transforms) {
    if (ArrayUtils.isEmpty(transforms)) {
      return Transforms.EMPTY_TRANSFORM;
    }

    return Arrays.stream(transforms)
        .filter(
            transform ->
                !((transform instanceof BucketTransform)
                    || (transform instanceof SortedBucketTransform)))
        .map(
            transform -> {
              if (transform instanceof IdentityTransform) {
                IdentityTransform identityTransform = (IdentityTransform) transform;
                return Transforms.identity(identityTransform.reference().fieldNames());
              } else {
                throw new NotSupportedException(
                    "Doesn't support Spark transform: " + transform.name());
              }
            })
        .toArray(Transform[]::new);
  }

  public static DistributionAndSortOrdersInfo toGravitinoDistributionAndSortOrders(
      org.apache.spark.sql.connector.expressions.Transform[] transforms) {
    DistributionAndSortOrdersInfo bundles = new DistributionAndSortOrdersInfo();
    if (ArrayUtils.isEmpty(transforms)) {
      return bundles;
    }

    Arrays.stream(transforms)
        .forEach(
            transform -> {
              if (transform instanceof SortedBucketTransform) {
                Pair<Distribution, SortOrder[]> pair =
                    toGravitinoDistributionAndSortOrders((SortedBucketTransform) transform);
                bundles.setDistribution(pair.getLeft());
                bundles.setSortOrders(pair.getRight());
              } else if (transform instanceof BucketTransform) {
                BucketTransform bucketTransform = (BucketTransform) transform;
                Distribution distribution = toGravitinoDistribution(bucketTransform);
                bundles.setDistribution(distribution);
              }
            });

    return bundles;
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
                Preconditions.checkArgument(
                    transform instanceof SingleFieldPartitioning,
                    "Only support SingleFieldPartitioning, but get " + transform.name());
                SingleFieldPartitioning identityTransform = (SingleFieldPartitioning) transform;
                String[] fieldName = identityTransform.fieldName();
                switch (identityTransform.strategy()) {
                  case IDENTITY:
                    sparkTransforms.add(
                        createSparkIdentityTransform(
                            String.join(ConnectorConstants.DOT, fieldName)));
                    break;
                  default:
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
        transToGravitinoNamedReference(
            JavaConverters.seqAsJavaList(sortedBucketTransform.columns()));

    Expression[] sortColumns =
        transToGravitinoNamedReference(
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

  private static Expression[] transToGravitinoNamedReference(
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
}
