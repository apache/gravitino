/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter.DistributionAndSortOrdersInfo;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.NotSupportedException;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.SortedBucketTransform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import scala.collection.JavaConverters;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSparkTransformConverter {
  private Map<org.apache.spark.sql.connector.expressions.Transform, Transform>
      sparkToGravitinoPartitionTransformMaps = new HashMap<>();

  @BeforeAll
  void init() {
    initSparkToGravitinoTransformMap();
  }

  @Test
  void testPartition() {
    sparkToGravitinoPartitionTransformMaps.forEach(
        (sparkTransform, gravitinoTransform) -> {
          Transform[] gravitinoPartitionings =
              SparkTransformConverter.toGravitinoPartitionings(
                  new org.apache.spark.sql.connector.expressions.Transform[] {sparkTransform});
          Assertions.assertTrue(
              gravitinoPartitionings != null && gravitinoPartitionings.length == 1);
          Assertions.assertEquals(gravitinoTransform, gravitinoPartitionings[0]);
        });

    sparkToGravitinoPartitionTransformMaps.forEach(
        (sparkTransform, gravitinoTransform) -> {
          org.apache.spark.sql.connector.expressions.Transform[] sparkTransforms =
              SparkTransformConverter.toSparkTransform(
                  new Transform[] {gravitinoTransform}, null, null);
          Assertions.assertTrue(sparkTransforms.length == 1);
          Assertions.assertEquals(sparkTransform, sparkTransforms[0]);
        });
  }

  @Test
  void testGravitinoToSparkDistributionWithoutSortOrder() {
    int bucketNum = 16;
    String[][] columnNames = createGravitinoFieldReferenceNames("a", "b.c");
    Distribution gravitinoDistribution = createHashDistribution(bucketNum, columnNames);

    org.apache.spark.sql.connector.expressions.Transform[] sparkTransforms =
        SparkTransformConverter.toSparkTransform(null, gravitinoDistribution, null);
    Assertions.assertTrue(sparkTransforms != null && sparkTransforms.length == 1);
    Assertions.assertTrue(sparkTransforms[0] instanceof BucketTransform);
    BucketTransform bucket = (BucketTransform) sparkTransforms[0];
    Assertions.assertEquals(bucketNum, (Integer) bucket.numBuckets().value());
    String[][] columns =
        JavaConverters.seqAsJavaList(bucket.columns()).stream()
            .map(namedReference -> namedReference.fieldNames())
            .toArray(String[][]::new);
    Assertions.assertArrayEquals(columnNames, columns);

    // none and null distribution
    sparkTransforms = SparkTransformConverter.toSparkTransform(null, null, null);
    Assertions.assertEquals(0, sparkTransforms.length);
    sparkTransforms = SparkTransformConverter.toSparkTransform(null, Distributions.NONE, null);
    Assertions.assertEquals(0, sparkTransforms.length);

    // range and even distribution
    Assertions.assertThrowsExactly(
        NotSupportedException.class,
        () -> SparkTransformConverter.toSparkTransform(null, Distributions.RANGE, null));
    Distribution evenDistribution = Distributions.even(bucketNum, NamedReference.field(""));
    Assertions.assertThrowsExactly(
        NotSupportedException.class,
        () -> SparkTransformConverter.toSparkTransform(null, evenDistribution, null));
  }

  @Test
  void testSparkToGravitinoDistributionWithoutSortOrder() {
    int bucketNum = 16;
    String[] sparkFieldReferences = new String[] {"a", "b.c"};

    org.apache.spark.sql.connector.expressions.Transform sparkBucket =
        Expressions.bucket(bucketNum, sparkFieldReferences);
    DistributionAndSortOrdersInfo distributionAndSortOrdersInfo =
        SparkTransformConverter.toGravitinoDistributionAndSortOrders(
            new org.apache.spark.sql.connector.expressions.Transform[] {sparkBucket});

    Assertions.assertNull(distributionAndSortOrdersInfo.getSortOrders());

    Distribution distribution = distributionAndSortOrdersInfo.getDistribution();
    String[][] gravitinoFieldReferences = createGravitinoFieldReferenceNames(sparkFieldReferences);
    Assertions.assertEquals(
        createHashDistribution(bucketNum, gravitinoFieldReferences), distribution);
  }

  @Test
  void testSparkToGravitinoDistributionWithSortOrder() {
    int bucketNum = 16;
    String[][] bucketColumnNames = createGravitinoFieldReferenceNames("a", "b.c");
    String[][] sortColumnNames = createGravitinoFieldReferenceNames("f", "m.n");
    SortedBucketTransform sortedBucketTransform =
        LogicalExpressions.bucket(
            bucketNum,
            createSparkFieldReference(bucketColumnNames),
            createSparkFieldReference(sortColumnNames));

    DistributionAndSortOrdersInfo distributionAndSortOrders =
        SparkTransformConverter.toGravitinoDistributionAndSortOrders(
            new org.apache.spark.sql.connector.expressions.Transform[] {sortedBucketTransform});
    Assertions.assertEquals(
        createHashDistribution(bucketNum, bucketColumnNames),
        distributionAndSortOrders.getDistribution());

    SortOrder[] sortOrders =
        createSortOrders(sortColumnNames, ConnectorConstants.SPARK_DEFAULT_SORT_DIRECTION);
    Assertions.assertArrayEquals(sortOrders, distributionAndSortOrders.getSortOrders());
  }

  @Test
  void testGravitinoToSparkDistributionWithSortOrder() {
    int bucketNum = 16;
    String[][] bucketColumnNames = createGravitinoFieldReferenceNames("a", "b.c");
    String[][] sortColumnNames = createGravitinoFieldReferenceNames("f", "m.n");
    Distribution distribution = createHashDistribution(bucketNum, bucketColumnNames);
    SortOrder[] sortOrders =
        createSortOrders(sortColumnNames, ConnectorConstants.SPARK_DEFAULT_SORT_DIRECTION);

    org.apache.spark.sql.connector.expressions.Transform[] transforms =
        SparkTransformConverter.toSparkTransform(null, distribution, sortOrders);
    Assertions.assertTrue(transforms.length == 1);
    Assertions.assertTrue(transforms[0] instanceof SortedBucketTransform);

    SortedBucketTransform sortedBucketTransform = (SortedBucketTransform) transforms[0];
    Assertions.assertEquals(bucketNum, (Integer) sortedBucketTransform.numBuckets().value());
    String[][] sparkSortColumns =
        JavaConverters.seqAsJavaList(sortedBucketTransform.sortedColumns()).stream()
            .map(sparkNamedReference -> sparkNamedReference.fieldNames())
            .toArray(String[][]::new);

    String[][] sparkBucketColumns =
        JavaConverters.seqAsJavaList(sortedBucketTransform.columns()).stream()
            .map(sparkNamedReference -> sparkNamedReference.fieldNames())
            .toArray(String[][]::new);

    Assertions.assertArrayEquals(bucketColumnNames, sparkBucketColumns);
    Assertions.assertArrayEquals(sortColumnNames, sparkSortColumns);
  }

  private org.apache.spark.sql.connector.expressions.NamedReference[] createSparkFieldReference(
      String[][] fields) {
    return Arrays.stream(fields)
        .map(field -> FieldReference.apply(String.join(ConnectorConstants.DOT, field)))
        .toArray(org.apache.spark.sql.connector.expressions.NamedReference[]::new);
  }

  // split column name for Gravitino
  private String[][] createGravitinoFieldReferenceNames(String... columnNames) {
    return Arrays.stream(columnNames)
        .map(columnName -> columnName.split("\\."))
        .toArray(String[][]::new);
  }

  private SortOrder[] createSortOrders(String[][] columnNames, SortDirection direction) {
    return Arrays.stream(columnNames)
        .map(columnName -> SortOrders.of(NamedReference.field(columnName), direction))
        .toArray(SortOrder[]::new);
  }

  private Distribution createHashDistribution(int bucketNum, String[][] columnNames) {
    NamedReference[] namedReferences =
        Arrays.stream(columnNames)
            .map(columnName -> NamedReference.field(columnName))
            .toArray(NamedReference[]::new);
    return Distributions.hash(bucketNum, namedReferences);
  }

  private void initSparkToGravitinoTransformMap() {
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkIdentityTransform("a"), Transforms.identity("a"));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkIdentityTransform("a.b"),
        Transforms.identity(new String[] {"a", "b"}));
  }
}
