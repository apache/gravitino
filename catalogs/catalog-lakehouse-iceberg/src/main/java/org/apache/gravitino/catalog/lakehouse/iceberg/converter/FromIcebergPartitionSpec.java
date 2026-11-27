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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
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
    return Transforms.identity(fieldName(sourceId));
  }

  @Override
  public Transform bucket(String sourceName, int sourceId, int numBuckets) {
    // String[]...: wrap as one nested path, not multiple top-level fields.
    return Transforms.bucket(numBuckets, new String[][] {fieldName(sourceId)});
  }

  @Override
  public Transform truncate(String sourceName, int sourceId, int width) {
    return Transforms.truncate(width, fieldName(sourceId));
  }

  @Override
  public Transform year(String sourceName, int sourceId) {
    return Transforms.year(fieldName(sourceId));
  }

  @Override
  public Transform month(String sourceName, int sourceId) {
    return Transforms.month(fieldName(sourceId));
  }

  @Override
  public Transform day(String sourceName, int sourceId) {
    return Transforms.day(fieldName(sourceId));
  }

  @Override
  public Transform hour(String sourceName, int sourceId) {
    return Transforms.hour(fieldName(sourceId));
  }

  private String[] fieldName(int sourceId) {
    return ConvertUtil.splitIcebergNameToFieldPath(idToName.get(sourceId));
  }

  @Override
  public Transform alwaysNull(int fieldId, String sourceName, int sourceId) {
    // Omit void from Table.partitioning(). Spark and Trino reject void(...). The native
    // partition-specs JSON keeps the field and its position, and a dropped source id is not
    // resolved here.
    return null;
  }

  @Override
  public Transform unknown(int fieldId, String sourceName, int sourceId, String transform) {
    throw new UnsupportedOperationException("Unsupported Transform conversion type.");
  }

  /**
   * Transform assembled into Gravitino using the given schema's id-to-name map.
   *
   * <p>Iceberg checks the current default spec against the current schema. {@code void} fields are
   * omitted, so a dropped source id is not resolved. Historical specs stay in the native {@code
   * partition-specs} JSON.
   *
   * @param partitionSpec Iceberg partition spec.
   * @param schema Iceberg schema.
   * @return array of transforms for partition fields. {@code void} fields are omitted.
   */
  public static Transform[] fromPartitionSpec(PartitionSpec partitionSpec, Schema schema) {
    return fromPartitionSpec(partitionSpec, schema, schema.idToName());
  }

  /**
   * Transform assembled into Gravitino.
   *
   * @param partitionSpec Iceberg partition spec.
   * @param schema Iceberg schema used by {@link PartitionSpecVisitor}.
   * @param idToName field-id to dotted Iceberg name for non-void fields.
   * @return array of transforms for partition fields. {@code void} fields are omitted.
   */
  public static Transform[] fromPartitionSpec(
      PartitionSpec partitionSpec, Schema schema, Map<Integer, String> idToName) {
    FromIcebergPartitionSpec visitor = new FromIcebergPartitionSpec(idToName);
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
