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

import static org.apache.gravitino.rel.expressions.transforms.Transforms.bucket;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.day;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.hour;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.month;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.truncate;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.rel.expressions.transforms.Transform;
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
