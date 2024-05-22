/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.transforms.SortOrderVisitor;

/**
 * Convert expressions of Iceberg SortOrders to function string.
 *
 * <p>Referred from org/apache/iceberg/spark/Spark3Util/DescribeSortOrderVisitor.java
 */
public class DescribeIcebergSortOrderVisitor implements SortOrderVisitor<String> {
  public static final DescribeIcebergSortOrderVisitor INSTANCE =
      new DescribeIcebergSortOrderVisitor();

  private DescribeIcebergSortOrderVisitor() {}

  @Override
  public String field(
      String sourceName,
      int sourceId,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return String.format("%s %s %s", sourceName, direction, nullOrder);
  }

  @Override
  public String bucket(
      String sourceName,
      int sourceId,
      int numBuckets,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return String.format("bucket(%s, %s) %s %s", numBuckets, sourceName, direction, nullOrder);
  }

  @Override
  public String truncate(
      String sourceName,
      int sourceId,
      int width,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return String.format("truncate(%s, %s) %s %s", sourceName, width, direction, nullOrder);
  }

  @Override
  public String year(
      String sourceName,
      int sourceId,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return String.format("years(%s) %s %s", sourceName, direction, nullOrder);
  }

  @Override
  public String month(
      String sourceName,
      int sourceId,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return String.format("months(%s) %s %s", sourceName, direction, nullOrder);
  }

  @Override
  public String day(
      String sourceName,
      int sourceId,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return String.format("days(%s) %s %s", sourceName, direction, nullOrder);
  }

  @Override
  public String hour(
      String sourceName,
      int sourceId,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return String.format("hours(%s) %s %s", sourceName, direction, nullOrder);
  }

  @Override
  public String unknown(
      String sourceName,
      int sourceId,
      String transform,
      org.apache.iceberg.SortDirection direction,
      NullOrder nullOrder) {
    return String.format("%s(%s) %s %s", transform, sourceName, direction, nullOrder);
  }
}
