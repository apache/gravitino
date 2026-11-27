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
package org.apache.gravitino.catalog.lakehouse.iceberg.utils;

import java.nio.file.Path;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.catalog.lakehouse.iceberg.converter.FromIcebergPartitionSpec;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.SupportsPartitionSpecs;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIcebergTablePropertiesUtil {
  @TempDir Path temporaryDirectory;

  @Test
  void testDefaultSpecComesFromMetadataAndTracksEvolution() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            temporaryDirectory.toUri().toString(),
            Map.of("default-spec-id", "999"));
    Assertions.assertEquals(0, metadata.defaultSpecId());
    Assertions.assertEquals(
        "0", IcebergTable.fromIcebergTable(metadata, "events").properties().get("default-spec-id"));

    TableMetadata evolved =
        TableMetadata.buildFrom(metadata)
            .setDefaultPartitionSpec(PartitionSpec.builderFor(schema).identity("id").build())
            .build();
    Assertions.assertEquals(1, evolved.defaultSpecId());
    Assertions.assertEquals(
        "1", IcebergTable.fromIcebergTable(evolved, "events").properties().get("default-spec-id"));
    Assertions.assertEquals(
        partitionSpecsJson(evolved),
        IcebergTable.fromIcebergTable(evolved, "events").properties().get("partition-specs"));
  }

  @Test
  void testBuilderHydratesDefaultPartitionSpec() {
    Transform[] partitioning = new Transform[] {Transforms.identity("id")};
    IcebergTable built =
        IcebergTable.builder()
            .withName("events")
            .withColumns(new IcebergColumn[0])
            .withPartitioning(partitioning)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    SupportsPartitionSpecs specs = built.supportsPartitionSpecs();
    Assertions.assertEquals(0, specs.defaultPartitionSpecId());
    Assertions.assertEquals(1, specs.partitionSpecs().size());
    Assertions.assertArrayEquals(built.partitioning(), specs.partitionSpecs().get(0));
    Assertions.assertNotSame(built.partitioning(), specs.partitionSpecs().get(0));
    Assertions.assertNotSame(partitioning, built.partitioning());
    Assertions.assertNotSame(partitioning, specs.partitionSpecs().get(0));
    partitioning[0] = Transforms.identity("mutated");
    Assertions.assertArrayEquals(new Transform[] {Transforms.identity("id")}, built.partitioning());
    Assertions.assertArrayEquals(
        new Transform[] {Transforms.identity("id")}, specs.partitionSpecs().get(0));
  }

  @Test
  void testBuilderNormalizesNullPartitioning() {
    IcebergTable built =
        IcebergTable.builder()
            .withName("events")
            .withColumns(new IcebergColumn[0])
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    Assertions.assertArrayEquals(Transforms.EMPTY_TRANSFORM, built.partitioning());
    SupportsPartitionSpecs specs = built.supportsPartitionSpecs();
    Assertions.assertEquals(0, specs.defaultPartitionSpecId());
    Assertions.assertArrayEquals(
        Transforms.EMPTY_TRANSFORM, specs.partitionSpecs().get(specs.defaultPartitionSpecId()));
  }

  @Test
  void testFromIcebergTableExposesAllPartitionSpecs() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), temporaryDirectory.toUri().toString(), Map.of());
    IcebergTable unpartitioned = IcebergTable.fromIcebergTable(metadata, "events");
    SupportsPartitionSpecs unpartitionedSpecs = unpartitioned.supportsPartitionSpecs();
    Assertions.assertEquals(0, unpartitionedSpecs.defaultPartitionSpecId());
    Assertions.assertEquals(1, unpartitionedSpecs.partitionSpecs().size());
    Assertions.assertArrayEquals(
        Transforms.EMPTY_TRANSFORM, unpartitionedSpecs.partitionSpecs().get(0));
    Assertions.assertArrayEquals(
        unpartitioned.partitioning(),
        unpartitionedSpecs.partitionSpecs().get(unpartitionedSpecs.defaultPartitionSpecId()));

    TableMetadata evolved =
        TableMetadata.buildFrom(metadata)
            .setDefaultPartitionSpec(PartitionSpec.builderFor(schema).identity("id").build())
            .build();
    IcebergTable partitioned = IcebergTable.fromIcebergTable(evolved, "events");
    SupportsPartitionSpecs specs = partitioned.supportsPartitionSpecs();
    Assertions.assertEquals(1, specs.defaultPartitionSpecId());
    Assertions.assertEquals(2, specs.partitionSpecs().size());
    Assertions.assertTrue(specs.partitionSpecs().containsKey(specs.defaultPartitionSpecId()));
    Assertions.assertArrayEquals(Transforms.EMPTY_TRANSFORM, specs.partitionSpecs().get(0));
    Transform[] identity = specs.partitionSpecs().get(1);
    Assertions.assertEquals(1, identity.length);
    Assertions.assertEquals(Transforms.identity("id"), identity[0]);
    Assertions.assertArrayEquals(partitioned.partitioning(), identity);
    Assertions.assertArrayEquals(
        partitioned.partitioning(), specs.partitionSpecs().get(specs.defaultPartitionSpecId()));
    Assertions.assertNotSame(
        partitioned.partitioning(), specs.partitionSpecs().get(specs.defaultPartitionSpecId()));
  }

  @Test
  void testFromIcebergTableLoadsAfterV2PartitionColumnDrop() {
    Schema initialSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()));
    PartitionSpec initialSpec =
        PartitionSpec.builderFor(initialSchema).identity("region").identity("id").build();
    TableMetadata metadata =
        TableMetadata.buildFromEmpty(2)
            .setLocation(temporaryDirectory.toUri().toString())
            .setCurrentSchema(initialSchema, 2)
            .setDefaultPartitionSpec(initialSpec)
            .setDefaultSortOrder(SortOrder.unsorted())
            .build();

    PartitionSpec evolvedSpec = PartitionSpec.builderFor(initialSchema).identity("id").build();
    TableMetadata afterSpecChange =
        TableMetadata.buildFrom(metadata).setDefaultPartitionSpec(evolvedSpec).build();
    Schema withoutRegion = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata afterDrop =
        TableMetadata.buildFrom(afterSpecChange).setCurrentSchema(withoutRegion, 2).build();

    Assertions.assertNull(afterDrop.schema().findField(2));
    Assertions.assertTrue(
        afterDrop.specsById().get(0).fields().stream().anyMatch(field -> field.sourceId() == 2));

    IcebergTable loaded = IcebergTable.fromIcebergTable(afterDrop, "events");
    SupportsPartitionSpecs specs = loaded.supportsPartitionSpecs();
    Assertions.assertEquals(1, specs.defaultPartitionSpecId());
    Assertions.assertEquals(2, specs.partitionSpecs().size());
    Transform[] historical = specs.partitionSpecs().get(0);
    Assertions.assertEquals(2, historical.length);
    Assertions.assertEquals(Transforms.identity("region"), historical[0]);
    Assertions.assertEquals(Transforms.identity("id"), historical[1]);
    Assertions.assertArrayEquals(
        new Transform[] {Transforms.identity("id")}, specs.partitionSpecs().get(1));
  }

  @Test
  void testFromIcebergTableKeepsV1VoidPartitionPositions() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()));
    PartitionSpec initialSpec =
        PartitionSpec.builderFor(schema).identity("region").identity("id").build();
    TableMetadata metadata =
        TableMetadata.buildFromEmpty(1)
            .setLocation(temporaryDirectory.toUri().toString())
            .setCurrentSchema(schema, 2)
            .setDefaultPartitionSpec(initialSpec)
            .setDefaultSortOrder(SortOrder.unsorted())
            .build();
    PartitionSpec voided =
        PartitionSpec.builderFor(schema).alwaysNull("region", "region").identity("id").build();
    TableMetadata evolved =
        TableMetadata.buildFrom(metadata).setDefaultPartitionSpec(voided).build();

    IcebergTable loaded = IcebergTable.fromIcebergTable(evolved, "events");
    Transform[] current = loaded.supportsPartitionSpecs().partitionSpecs().get(1);
    Assertions.assertEquals(2, current.length);
    Assertions.assertEquals(FromIcebergPartitionSpec.VOID_TRANSFORM, current[0].name());
    Assertions.assertEquals(Transforms.identity("id"), current[1]);
    Assertions.assertArrayEquals(loaded.partitioning(), current);
    Assertions.assertEquals(
        partitionSpecsJson(evolved), loaded.properties().get("partition-specs"));
  }

  private static String partitionSpecsJson(TableMetadata metadata) {
    return "["
        + PartitionSpecParser.toJson(metadata.specsById().get(0))
        + ","
        + PartitionSpecParser.toJson(metadata.spec())
        + "]";
  }
}
