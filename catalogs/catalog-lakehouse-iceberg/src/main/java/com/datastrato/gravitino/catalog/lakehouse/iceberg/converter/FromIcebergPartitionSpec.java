/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.PartitionSpecVisitor;

/**
 * Convert IcebergTransform to GravitinoTransform.
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
    return Transforms.identity(idToName.get(sourceId));
  }

  @Override
  public Transform bucket(String sourceName, int sourceId, int numBuckets) {
    return Transforms.bucket(numBuckets, new String[] {idToName.get(sourceId)});
  }

  @Override
  public Transform truncate(String sourceName, int sourceId, int width) {
    return Transforms.truncate(width, idToName.get(sourceId));
  }

  @Override
  public Transform year(String sourceName, int sourceId) {
    return Transforms.year(idToName.get(sourceId));
  }

  @Override
  public Transform month(String sourceName, int sourceId) {
    return Transforms.month(idToName.get(sourceId));
  }

  @Override
  public Transform day(String sourceName, int sourceId) {
    return Transforms.day(idToName.get(sourceId));
  }

  @Override
  public Transform hour(String sourceName, int sourceId) {
    return Transforms.hour(idToName.get(sourceId));
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
   * Transform assembled into gravitino.
   *
   * @param partitionSpec
   * @param schema
   * @return array of transforms for partition fields.
   */
  public static Transform[] fromPartitionSpec(PartitionSpec partitionSpec, Schema schema) {
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
