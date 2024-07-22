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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link FromIcebergPartitionSpec}. */
public class TestFromIcebergPartitionSpec extends TestBaseConvert {

  @Test
  public void testFormTransform() {
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
