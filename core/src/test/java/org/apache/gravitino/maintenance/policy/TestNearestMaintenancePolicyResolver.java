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

package org.apache.gravitino.maintenance.policy;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestNearestMaintenancePolicyResolver {

  private static final String METALAKE = "ml";
  private static final MetadataObject TABLE =
      MetadataObjects.of(ImmutableList.of("cat", "db", "t"), MetadataObject.Type.TABLE);

  private PolicyDispatcher dispatcher;
  private NearestMaintenancePolicyResolver resolver;
  private MockedStatic<GravitinoEnv> gravitinoEnv;

  @BeforeEach
  void setUp() {
    dispatcher = Mockito.mock(PolicyDispatcher.class);
    resolver = new NearestMaintenancePolicyResolver(dispatcher);
    GravitinoEnv env = mock(GravitinoEnv.class);
    TableDispatcher tableDispatcher = mock(TableDispatcher.class);
    when(env.internalTableDispatcher()).thenReturn(tableDispatcher);
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    gravitinoEnv = mockStatic(GravitinoEnv.class);
    gravitinoEnv.when(GravitinoEnv::getInstance).thenReturn(env);
  }

  @AfterEach
  void tearDown() {
    if (gravitinoEnv != null) {
      gravitinoEnv.close();
    }
  }

  @Test
  void testPrefersDirectTablePolicyOverCatalog() {
    PolicyEntity tablePolicy = compactionPolicy("table-policy", 11L);
    PolicyEntity catalogPolicy = compactionPolicy("catalog-policy", 22L);

    Mockito.when(dispatcher.listDirectPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(new PolicyEntity[] {tablePolicy});
    Mockito.when(dispatcher.listDirectPolicyInfosForMetadataObject(METALAKE, catalogParent()))
        .thenReturn(new PolicyEntity[] {catalogPolicy});
    Mockito.when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(new PolicyEntity[] {catalogPolicy});

    PolicyEntity resolved =
        resolver.resolveNearest(METALAKE, TABLE, TableMaintenanceTaskType.COMPACTION).orElseThrow();
    Assertions.assertEquals(11L, resolved.id());
  }

  @Test
  void testEffectivePolicyResolvesMinInterval() {
    TableMaintenancePolicyFields fields =
        new TableMaintenancePolicyFields(new TableMaintenanceSchedule(true, null), 42_000L, null);
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(7L)
            .withName("p")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withEnabled(true)
            .withContent(
                PolicyContents.icebergDataCompaction(
                    1000L, 1L, 1L, 100L, 50L, ImmutableMap.of(), fields))
            .withAuditInfo(
                AuditInfo.builder().withCreator("u").withCreateTime(Instant.now()).build())
            .build();

    Mockito.when(dispatcher.listDirectPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(new PolicyEntity[] {policy});
    Mockito.when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(new PolicyEntity[] {});

    EffectiveTableMaintenancePolicy effective =
        EffectiveTableMaintenancePolicy.resolve(
                resolver, METALAKE, TABLE, TableMaintenanceTaskType.COMPACTION, ImmutableMap.of())
            .orElseThrow();

    Assertions.assertEquals(42_000L, effective.minIntervalMs());
    Assertions.assertEquals(fields, effective.maintenanceFields());
  }

  private static MetadataObject catalogParent() {
    return MetadataObjects.of(ImmutableList.of("cat"), MetadataObject.Type.CATALOG);
  }

  private static PolicyEntity compactionPolicy(String name, long id) {
    return PolicyEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
        .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
        .withEnabled(true)
        .withContent(PolicyContents.icebergDataCompaction())
        .withAuditInfo(AuditInfo.builder().withCreator("u").withCreateTime(Instant.now()).build())
        .build();
  }
}
