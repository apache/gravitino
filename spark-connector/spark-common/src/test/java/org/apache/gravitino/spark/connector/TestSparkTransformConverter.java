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

package org.apache.gravitino.spark.connector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.spark.connector.SparkTransformConverter.DistributionAndSortOrdersInfo;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.SortedBucketTransform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.JavaConverters;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSparkTransformConverter {
  private Map<org.apache.spark.sql.connector.expressions.Transform, Transform>
      sparkToGravitinoPartitionTransformMaps = new HashMap<>();

  @BeforeAll
  void init() {
    initSparkToGravitinoTransformMap();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testPartition(boolean supportsBucketPartition) {
    SparkTransformConverter sparkTransformConverter =
        new SparkTransformConverter(supportsBucketPartition);

    sparkToGravitinoPartitionTransformMaps.forEach(
        (sparkTransform, gravitinoTransform) -> {
          Transform[] gravitinoPartitionings =
              sparkTransformConverter.toGravitinoPartitionings(
                  new org.apache.spark.sql.connector.expressions.Transform[] {sparkTransform});
          if (sparkTransform instanceof BucketTransform && !supportsBucketPartition) {
            Assertions.assertTrue(
                gravitinoPartitionings != null && gravitinoPartitionings.length == 0);
          } else {
            Assertions.assertTrue(
                gravitinoPartitionings != null && gravitinoPartitionings.length == 1);
            Assertions.assertEquals(gravitinoTransform, gravitinoPartitionings[0]);
          }
        });

    sparkToGravitinoPartitionTransformMaps.forEach(
        (sparkTransform, gravitinoTransform) -> {
          org.apache.spark.sql.connector.expressions.Transform[] sparkTransforms =
              sparkTransformConverter.toSparkTransform(
                  new Transform[] {gravitinoTransform}, null, null);
          Assertions.assertEquals(1, sparkTransforms.length);
          Assertions.assertEquals(sparkTransform, sparkTransforms[0]);
        });
  }

  @SuppressWarnings("deprecation")
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testGravitinoToSparkDistributionWithoutSortOrder(boolean supportsBucketPartition) {
    SparkTransformConverter sparkTransformConverter =
        new SparkTransformConverter(supportsBucketPartition);
    int bucketNum = 16;
    String[][] columnNames = createGravitinoFieldReferenceNames("a", "b.c");
    Distribution gravitinoDistribution = createHashDistribution(bucketNum, columnNames);

    org.apache.spark.sql.connector.expressions.Transform[] sparkTransforms =
        sparkTransformConverter.toSparkTransform(null, gravitinoDistribution, null);
    if (supportsBucketPartition) {
      Assertions.assertTrue(sparkTransforms != null && sparkTransforms.length == 0);
    } else {
      Assertions.assertTrue(sparkTransforms != null && sparkTransforms.length == 1);
      Assertions.assertTrue(sparkTransforms[0] instanceof BucketTransform);
      BucketTransform bucket = (BucketTransform) sparkTransforms[0];
      Assertions.assertEquals(bucketNum, (Integer) bucket.numBuckets().value());
      String[][] columns =
          JavaConverters.seqAsJavaList(bucket.columns()).stream()
              .map(namedReference -> namedReference.fieldNames())
              .toArray(String[][]::new);
      Assertions.assertArrayEquals(columnNames, columns);
    }

    // none and null distribution
    sparkTransforms = sparkTransformConverter.toSparkTransform(null, null, null);
    Assertions.assertEquals(0, sparkTransforms.length);
    sparkTransforms = sparkTransformConverter.toSparkTransform(null, Distributions.NONE, null);
    Assertions.assertEquals(0, sparkTransforms.length);

    if (!supportsBucketPartition) {
      // range and even distribution
      Assertions.assertThrowsExactly(
          UnsupportedOperationException.class,
          () -> sparkTransformConverter.toSparkTransform(null, Distributions.RANGE, null));
      Distribution evenDistribution = Distributions.even(bucketNum, NamedReference.field(""));
      Assertions.assertThrowsExactly(
          UnsupportedOperationException.class,
          () -> sparkTransformConverter.toSparkTransform(null, evenDistribution, null));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSparkToGravitinoDistributionWithoutSortOrder(boolean supportsBucketPartition) {
    SparkTransformConverter sparkTransformConverter =
        new SparkTransformConverter(supportsBucketPartition);
    int bucketNum = 16;
    String[] sparkFieldReferences = new String[] {"a", "b.c"};

    org.apache.spark.sql.connector.expressions.Transform sparkBucket =
        Expressions.bucket(bucketNum, sparkFieldReferences);
    DistributionAndSortOrdersInfo distributionAndSortOrdersInfo =
        sparkTransformConverter.toGravitinoDistributionAndSortOrders(
            new org.apache.spark.sql.connector.expressions.Transform[] {sparkBucket});

    if (!supportsBucketPartition) {
      Distribution distribution = distributionAndSortOrdersInfo.getDistribution();
      String[][] gravitinoFieldReferences =
          createGravitinoFieldReferenceNames(sparkFieldReferences);
      Assertions.assertEquals(
          createHashDistribution(bucketNum, gravitinoFieldReferences), distribution);
    } else {
      Assertions.assertNotNull(distributionAndSortOrdersInfo.getSortOrders());
      Assertions.assertEquals(0, distributionAndSortOrdersInfo.getSortOrders().length);
      Assertions.assertNotNull(distributionAndSortOrdersInfo.getDistribution());
      Assertions.assertEquals(Distributions.NONE, distributionAndSortOrdersInfo.getDistribution());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSparkToGravitinoDistributionWithSortOrder(boolean supportsBucketPartition) {
    SparkTransformConverter sparkTransformConverter =
        new SparkTransformConverter(supportsBucketPartition);

    int bucketNum = 16;
    String[][] bucketColumnNames = createGravitinoFieldReferenceNames("a", "b.c");
    String[][] sortColumnNames = createGravitinoFieldReferenceNames("f", "m.n");
    SortedBucketTransform sortedBucketTransform =
        LogicalExpressions.bucket(
            bucketNum,
            createSparkFieldReference(bucketColumnNames),
            createSparkFieldReference(sortColumnNames));

    if (!supportsBucketPartition) {
      DistributionAndSortOrdersInfo distributionAndSortOrders =
          sparkTransformConverter.toGravitinoDistributionAndSortOrders(
              new org.apache.spark.sql.connector.expressions.Transform[] {sortedBucketTransform});

      Assertions.assertEquals(
          createHashDistribution(bucketNum, bucketColumnNames),
          distributionAndSortOrders.getDistribution());

      SortOrder[] sortOrders =
          createSortOrders(sortColumnNames, ConnectorConstants.SPARK_DEFAULT_SORT_DIRECTION);
      Assertions.assertArrayEquals(sortOrders, distributionAndSortOrders.getSortOrders());
    } else {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              sparkTransformConverter.toGravitinoDistributionAndSortOrders(
                  new org.apache.spark.sql.connector.expressions.Transform[] {
                    sortedBucketTransform
                  }));
    }
  }

  @SuppressWarnings("deprecation")
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testGravitinoToSparkDistributionWithSortOrder(boolean supportsBucketPartition) {
    SparkTransformConverter sparkTransformConverter =
        new SparkTransformConverter(supportsBucketPartition);
    int bucketNum = 16;
    String[][] bucketColumnNames = createGravitinoFieldReferenceNames("a", "b.c");
    String[][] sortColumnNames = createGravitinoFieldReferenceNames("f", "m.n");
    Distribution distribution = createHashDistribution(bucketNum, bucketColumnNames);
    SortOrder[] sortOrders =
        createSortOrders(sortColumnNames, ConnectorConstants.SPARK_DEFAULT_SORT_DIRECTION);

    org.apache.spark.sql.connector.expressions.Transform[] transforms =
        sparkTransformConverter.toSparkTransform(null, distribution, sortOrders);
    if (!supportsBucketPartition) {
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
    } else {
      Assertions.assertEquals(0, transforms.length);
    }
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
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkBucketTransform(10, new String[] {"a"}),
        Transforms.bucket(10, new String[] {"a"}));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkBucketTransform(10, new String[] {"msg.a"}),
        Transforms.bucket(10, new String[] {"msg", "a"}));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkHoursTransform(NamedReference.field("date")),
        Transforms.hour("date"));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkHoursTransform(NamedReference.field("msg.date")),
        Transforms.hour(new String[] {"msg", "date"}));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkDaysTransform(NamedReference.field("date")),
        Transforms.day("date"));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkDaysTransform(NamedReference.field("msg.date")),
        Transforms.day(new String[] {"msg", "date"}));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkMonthsTransform(NamedReference.field("date")),
        Transforms.month("date"));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkMonthsTransform(NamedReference.field("msg.date")),
        Transforms.month(new String[] {"msg", "date"}));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkYearsTransform(NamedReference.field("date")),
        Transforms.year("date"));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkYearsTransform(NamedReference.field("msg.date")),
        Transforms.year(new String[] {"msg", "date"}));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkTruncateTransform(10, new String[] {"package"}),
        Transforms.truncate(10, "package"));
    sparkToGravitinoPartitionTransformMaps.put(
        SparkTransformConverter.createSparkTruncateTransform(10, new String[] {"msg.package"}),
        Transforms.truncate(10, "msg.package"));
  }
}
