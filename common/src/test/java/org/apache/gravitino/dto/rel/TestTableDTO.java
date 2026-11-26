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
package org.apache.gravitino.dto.rel;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.rel.SupportsPartitionSpecs;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableDTO {
  @Test
  public void testBuildWithoutColumns() {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    TableDTO.Builder<?> builder = TableDTO.builder().withName("t1").withAudit(audit);
    Assertions.assertDoesNotThrow(
        () -> {
          builder.build();
        });
  }

  @Test
  public void testBuildWithEmptyColumns() {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    TableDTO.Builder<?> builder =
        TableDTO.builder().withName("t1").withAudit(audit).withColumns(new ColumnDTO[0]);

    Assertions.assertDoesNotThrow(
        () -> {
          builder.build();
        });
  }

  @Test
  public void testBuildWithColumns() {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    ColumnDTO column =
        ColumnDTO.builder().withName("c1").withDataType(Types.IntegerType.get()).build();
    TableDTO.Builder<?> builder =
        TableDTO.builder().withName("t1").withAudit(audit).withColumns(new ColumnDTO[] {column});
    assertDoesNotThrow(builder::build);
  }

  @Test
  public void testToDTOPreservesPartitionSpecsThroughSnapshot() {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    Partitioning[] identity = new Partitioning[] {IdentityPartitioningDTO.of("id")};
    Map<Integer, Partitioning[]> specs = ImmutableMap.of(0, new Partitioning[0], 1, identity);
    TableDTO source =
        TableDTO.builder()
            .withName("events")
            .withAudit(audit)
            .withColumns(new ColumnDTO[0])
            .withPartitioning(identity)
            .withDefaultPartitionSpecId(1)
            .withPartitionSpecs(specs)
            .build();

    Table snapshot = DTOConverters.toDTO(source);
    SupportsPartitionSpecs partitionSpecs = snapshot.supportsPartitionSpecs();
    Assertions.assertEquals(1, partitionSpecs.defaultPartitionSpecId());
    Assertions.assertEquals(2, partitionSpecs.partitionSpecs().size());
    Assertions.assertArrayEquals(
        Transforms.EMPTY_TRANSFORM, partitionSpecs.partitionSpecs().get(0));
    Transform[] defaultSpec = partitionSpecs.partitionSpecs().get(1);
    Assertions.assertEquals(1, defaultSpec.length);
    Assertions.assertInstanceOf(IdentityPartitioningDTO.class, defaultSpec[0]);
    Assertions.assertArrayEquals(
        new String[] {"id"}, ((IdentityPartitioningDTO) defaultSpec[0]).fieldName());
  }

  @Test
  public void testSupportsPartitionSpecsAbsentByDefault() {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    TableDTO table =
        TableDTO.builder().withName("t1").withAudit(audit).withColumns(new ColumnDTO[0]).build();
    Assertions.assertThrows(UnsupportedOperationException.class, table::supportsPartitionSpecs);
  }
}
