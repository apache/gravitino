/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link FromIcebergPartitionSpec}. */
public class TestFromIcebergPartitionSpec extends TestBaseConvert {

  @Test
  void testFormTransform() {
    Types.NestedField[] nestedFields = createNestedField("col_1", "col_2", "col_3");
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(4, "col_4", Types.DateType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(5, "col_5", Types.DateType.get()));
    nestedFields =
        ArrayUtils.add(
            nestedFields, createNestedField(6, "col_6", Types.TimestampType.withoutZone()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(7, "col_7", Types.IntegerType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(8, "col_8", Types.IntegerType.get()));

    Schema schema = new Schema(nestedFields);
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    PartitionSpec partitionSpec =
        builder
            .year("col_4")
            .day("col_5")
            .hour("col_6")
            .bucket("col_7", 5)
            .truncate("col_8", 8)
            .identity("col_1")
            .build();
    Transform[] transforms = FromIcebergPartitionSpec.fromPartitionSpec(partitionSpec, schema);
    Assertions.assertEquals(6, transforms.length);

    List<PartitionField> fields = partitionSpec.fields();
    Map<Integer, String> idToName = schema.idToName();
    Map<String, String> transformMapping =
        fields.stream()
            .collect(
                Collectors.toMap(
                    PartitionField::name,
                    partitionField -> idToName.get(partitionField.sourceId())));

    for (Transform transform : transforms) {
      if (transform instanceof Transforms.TruncateTransform) {
        String colName = ((Transforms.TruncateTransform) transform).fieldName()[0];
        String transformKey =
            String.join(
                "_", colName, "truncate".equals(transform.name()) ? "trunc" : transform.name());
        Assertions.assertTrue(transformMapping.containsKey(transformKey));
        Assertions.assertEquals(transformMapping.get(transformKey), colName);
      } else if (transform instanceof Transforms.IdentityTransform) {
        Assertions.assertEquals(
            "col_1", ((Transform.SingleFieldTransform) transform).fieldName()[0]);
      }
    }
  }
}
