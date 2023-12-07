/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.bucket;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.day;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.hour;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.identity;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.month;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.truncate;

import com.datastrato.gravitino.rel.expressions.transforms.Transform;
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
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(8, "col_8", Types.LongType.get()));
    nestedFields =
        ArrayUtils.add(nestedFields, createNestedField(9, "col_9", Types.StringType.get()));

    Transform[] partitioning =
        new Transform[] {
          identity(nestedFields[0].name()),
          day(nestedFields[4].name()),
          hour(nestedFields[5].name()),
          month(nestedFields[6].name()),
          bucket(10, new String[] {nestedFields[4].name()}),
          truncate(20, nestedFields[7].name()),
        };

    Schema schema = new Schema(nestedFields);

    PartitionSpec partitionSpec = ToIcebergPartitionSpec.toPartitionSpec(schema, partitioning);

    List<PartitionField> fields = partitionSpec.fields();
    Assertions.assertEquals(partitioning.length, fields.size());
    Assertions.assertEquals(6, fields.size());
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
