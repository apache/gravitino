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

  @Test
  public void testNestedHyphenatedIdentityKeepsFieldComponents() {
    // Iceberg idToName is "event.user-id"; Gravitino must keep ["event", "user-id"], not one
    // dotted component.
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "event",
                Types.StructType.of(
                    Types.NestedField.required(2, "user-id", Types.IntegerType.get()))));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("event.user-id").build();

    Transform[] transforms = FromIcebergPartitionSpec.fromPartitionSpec(partitionSpec, schema);

    Assertions.assertEquals(1, transforms.length);
    Assertions.assertInstanceOf(Transforms.IdentityTransform.class, transforms[0]);
    Assertions.assertArrayEquals(
        new String[] {"event", "user-id"},
        ((Transforms.IdentityTransform) transforms[0]).fieldName());
    Assertions.assertEquals("event.user-id", schema.idToName().get(2));
  }

  @Test
  public void testNestedHyphenatedDayKeepsFieldComponents() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "event",
                Types.StructType.of(
                    Types.NestedField.required(2, "event-ts", Types.TimestampType.withoutZone()))));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).day("event.event-ts").build();

    Transform[] transforms = FromIcebergPartitionSpec.fromPartitionSpec(partitionSpec, schema);

    Assertions.assertEquals(1, transforms.length);
    Assertions.assertInstanceOf(Transforms.DayTransform.class, transforms[0]);
    Assertions.assertArrayEquals(
        new String[] {"event", "event-ts"}, ((Transforms.DayTransform) transforms[0]).fieldName());
  }

  @Test
  public void testSplitIcebergNameToFieldPath() {
    Assertions.assertArrayEquals(
        new String[] {"col_1"}, ConvertUtil.splitIcebergNameToFieldPath("col_1"));
    Assertions.assertArrayEquals(
        new String[] {"event", "user-id"},
        ConvertUtil.splitIcebergNameToFieldPath("event.user-id"));
    Assertions.assertArrayEquals(
        new String[] {"event", "meta", "user-id"},
        ConvertUtil.splitIcebergNameToFieldPath("event.meta.user-id"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ConvertUtil.splitIcebergNameToFieldPath(""));
  }

  @Test
  public void testV1VoidPartitionFieldIsOmitted() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).alwaysNull("region", "region").identity("id").build();

    Transform[] transforms =
        FromIcebergPartitionSpec.fromPartitionSpec(
            partitionSpec, new Schema(schema.findField("id")));

    Assertions.assertArrayEquals(new Transform[] {Transforms.identity("id")}, transforms);
  }
}
