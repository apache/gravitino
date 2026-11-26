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
package org.apache.gravitino.client;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.rel.SupportsPartitionSpecs;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Verifies {@link RelationalTable} preserves historical partition specs from {@link TableDTO}. */
public class TestRelationalTablePartitionSpecs extends TestBase {

  private static final String METALAKE = "metalake_partition_specs";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    TestGravitinoMetalake.createMetalake(client, METALAKE);
  }

  @Test
  public void testSupportsPartitionSpecsFromLoadedTableDTO() {
    Partitioning[] identity = new Partitioning[] {IdentityPartitioningDTO.of("id")};
    TableDTO tableDTO =
        TableDTO.builder()
            .withName("events")
            .withComment("comment")
            .withColumns(
                new ColumnDTO[] {
                  ColumnDTO.builder().withName("id").withDataType(Types.LongType.get()).build()
                })
            .withProperties(Collections.emptyMap())
            .withPartitioning(identity)
            .withDefaultPartitionSpecId(1)
            .withPartitionSpecs(ImmutableMap.of(0, new Partitioning[0], 1, identity))
            .withAudit(AuditDTO.builder().withCreator("test").build())
            .build();

    RelationalTable table =
        RelationalTable.from(
            Namespace.of(METALAKE, "catalog1", "schema1"), tableDTO, client.restClient());

    SupportsPartitionSpecs specs = table.supportsPartitionSpecs();
    Assertions.assertEquals(1, specs.defaultPartitionSpecId());
    Assertions.assertEquals(2, specs.partitionSpecs().size());
    Assertions.assertArrayEquals(Transforms.EMPTY_TRANSFORM, specs.partitionSpecs().get(0));
    Transform[] defaultSpec = specs.partitionSpecs().get(1);
    Assertions.assertEquals(1, defaultSpec.length);
    Assertions.assertInstanceOf(IdentityPartitioningDTO.class, defaultSpec[0]);
  }

  @Test
  public void testSupportsPartitionSpecsAbsentWithoutDTOFields() {
    TableDTO tableDTO =
        TableDTO.builder()
            .withName("events")
            .withColumns(
                new ColumnDTO[] {
                  ColumnDTO.builder().withName("id").withDataType(Types.LongType.get()).build()
                })
            .withProperties(Collections.emptyMap())
            .withAudit(AuditDTO.builder().withCreator("test").build())
            .build();

    RelationalTable table =
        RelationalTable.from(
            Namespace.of(METALAKE, "catalog1", "schema1"), tableDTO, client.restClient());

    Assertions.assertThrows(UnsupportedOperationException.class, table::supportsPartitionSpecs);
  }
}
