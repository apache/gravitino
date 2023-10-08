/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.PartitionSpecVisitor;

import static io.substrait.expression.ExpressionCreator.i32;

/**
 * Convert IcebergTransform to GravitonTransform.
 *
 * <p>Referred from
 * core/src/main/java/org/apache/iceberg/spark/Spark3Util/SpecTransformToSparkTransform.java
 */
public class FromIcebergPartitionSpec implements PartitionSpecVisitor<Transform> {

  private final Map<Integer, String> idToName;

  public FromIcebergPartitionSpec(Map<Integer, String> idToName) {
    this.idToName = idToName;
  }

  @Override
  public Transform identity(String sourceName, int sourceId) {
    return Transforms.identity(new String[] {idToName.get(sourceId)});
  }

  @Override
  public Transform bucket(String sourceName, int sourceId, int numBuckets) {
    Transforms.LiteralReference bucketNum = Transforms.literal(i32(false, numBuckets));
    Transforms.NamedReference field = Transforms.field(new String[]{idToName.get(sourceId)});
    // bucket(fieldName, bucketNum)
    return Transforms.function("bucket", new Transform[]{field, bucketNum});
  }

  @Override
  public Transform truncate(String sourceName, int sourceId, int width) {
    // TODO @minghuang Need to implement truncate type conversion
    throw new UnsupportedOperationException();
  }

  @Override
  public Transform year(String sourceName, int sourceId) {
    return Transforms.year(new String[] {idToName.get(sourceId)});
  }

  @Override
  public Transform month(String sourceName, int sourceId) {
    return Transforms.month(new String[] {idToName.get(sourceId)});
  }

  @Override
  public Transform day(String sourceName, int sourceId) {
    return Transforms.day(new String[] {idToName.get(sourceId)});
  }

  @Override
  public Transform hour(String sourceName, int sourceId) {
    return Transforms.hour(new String[] {idToName.get(sourceId)});
  }

  @Override
  public Transform alwaysNull(int fieldId, String sourceName, int sourceId) {
    // do nothing for alwaysNull, it doesn't need to be converted to a transform
    return null;
  }

  @Override
  public Transform unknown(int fieldId, String sourceName, int sourceId, String transform) {
    throw new UnsupportedOperationException("Unsupported Transform conversion type.");
  }

  /**
   * Transform assembled into graviton.
   *
   * @param partitionSpec
   * @param schema
   * @return
   */
  @VisibleForTesting
  public static Transform[] formPartitionSpec(PartitionSpec partitionSpec, Schema schema) {
    FromIcebergPartitionSpec visitor = new FromIcebergPartitionSpec(schema.idToName());
    List<Transform> transforms = Lists.newArrayList();
    List<PartitionField> fields = partitionSpec.fields();

    for (PartitionField field : fields) {
      Transform transform = PartitionSpecVisitor.visit(schema, field, visitor);
      if (transform != null) {
        transforms.add(transform);
      }
    }
    return transforms.toArray(new Transform[0]);
  }
}
