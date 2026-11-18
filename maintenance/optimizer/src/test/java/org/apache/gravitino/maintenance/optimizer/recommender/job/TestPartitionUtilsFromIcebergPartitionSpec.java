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
package org.apache.gravitino.maintenance.optimizer.recommender.job;

import java.util.List;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergPartitionSpec;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression for the production catalog path: Iceberg {@link PartitionSpec} → {@link
 * FromIcebergPartitionSpec} → {@link PartitionUtils} (not a hand-built multi-component transform).
 */
public class TestPartitionUtilsFromIcebergPartitionSpec {

  @Test
  void testWhereClauseFromConvertedNestedHyphenatedIdentity() {
    Schema schema =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1,
                "event",
                org.apache.iceberg.types.Types.StructType.of(
                    org.apache.iceberg.types.Types.NestedField.required(
                        2, "user-id", org.apache.iceberg.types.Types.IntegerType.get()))));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("event.user-id").build();
    Transform[] transforms = FromIcebergPartitionSpec.fromPartitionSpec(partitionSpec, schema);

    Column[] columns =
        new Column[] {
          ColumnDTO.builder()
              .withName("event")
              .withDataType(
                  Types.StructType.of(
                      Types.StructType.Field.notNullField("user-id", Types.IntegerType.get())))
              .build()
        };
    List<PartitionEntry> partitions = List.of(new PartitionEntryImpl("event.user-id", "7"));

    String where =
        PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms, "ice");

    Assertions.assertEquals("`event`.`user-id` = 7", where);
  }
}
