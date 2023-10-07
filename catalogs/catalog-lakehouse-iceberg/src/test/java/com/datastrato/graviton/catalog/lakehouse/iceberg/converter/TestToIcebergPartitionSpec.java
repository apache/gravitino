/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import static com.datastrato.graviton.rel.transforms.Transforms.day;
import static com.datastrato.graviton.rel.transforms.Transforms.hour;
import static com.datastrato.graviton.rel.transforms.Transforms.month;

import com.datastrato.graviton.rel.transforms.Transform;
import java.util.Arrays;
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

/** Test class for {@link ToIcebergPartitionSpec}. */
public class TestToIcebergPartitionSpec extends TestBaseConvert {

  @Test
  void testToPartitionSpec() {
    Types.NestedField[] nestedFields = createNestedField("col_1", "col_2", "col_3", "col_4");

    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(5, "col_5", Types.DateType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(6, "col_6", Types.TimestampType.withZone()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(7, "col_7", Types.DateType.get()));

    Transform[] partitioning =
        new Transform[] {
          day(new String[] {nestedFields[4].name()}),
          hour(new String[] {nestedFields[5].name()}),
          month(new String[] {nestedFields[6].name()})
        };

    Schema schema = new Schema(nestedFields);

    PartitionSpec partitionSpec = ToIcebergPartitionSpec.toPartitionSpec(schema, partitioning);

    List<PartitionField> fields = partitionSpec.fields();
    Assertions.assertEquals(partitioning.length, fields.size());
    Assertions.assertEquals(3, fields.size());
    Map<Integer, String> idToName = schema.idToName();
    Map<String, Types.NestedField> nestedFieldByName =
        Arrays.stream(nestedFields).collect(Collectors.toMap(Types.NestedField::name, v -> v));
    for (int i = 0; i < fields.size(); i++) {
      PartitionField partitionField = fields.get(i);
      Assertions.assertTrue(idToName.containsKey(partitionField.sourceId()));
      String colName = idToName.get(partitionField.sourceId());
      Assertions.assertTrue(nestedFieldByName.containsKey(colName));
    }
  }
}
