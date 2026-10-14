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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
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
  private static final MetadataObject CATALOG =
      MetadataObjects.of(ImmutableList.of("cat"), MetadataObject.Type.CATALOG);
  private static final MetadataObject SCHEMA =
      MetadataObjects.of(ImmutableList.of("cat", "db"), MetadataObject.Type.SCHEMA);
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
    SchemaDispatcher schemaDispatcher = mock(SchemaDispatcher.class);
    CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);
    when(env.internalTableDispatcher()).thenReturn(tableDispatcher);
    when(env.internalSchemaDispatcher()).thenReturn(schemaDispatcher);
    when(env.internalCatalogDispatcher()).thenReturn(catalogDispatcher);
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    when(schemaDispatcher.schemaExists(any())).thenReturn(true);
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);
    gravitinoEnv = mockStatic(GravitinoEnv.class);
    gravitinoEnv.when(GravitinoEnv::getInstance).thenReturn(env);

    when(dispatcher.listPolicyInfosForMetadataObject(eq(METALAKE), any()))
        .thenReturn(new PolicyEntity[0]);
  }

  @AfterEach
  void tearDown() {
    if (gravitinoEnv != null) {
      gravitinoEnv.close();
    }
  }

  @Test
  void testPrefersTableOverSchemaAndCatalog() {
    PolicyEntity tablePolicy = compactionPolicy("table-policy", 11L).copyWithInherited(false);
    PolicyEntity schemaPolicy = compactionPolicy("schema-policy", 22L).copyWithInherited(false);
    PolicyEntity catalogPolicy = compactionPolicy("catalog-policy", 33L).copyWithInherited(false);

    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(
            new PolicyEntity[] {
              schemaPolicy.copyWithInherited(true),
              catalogPolicy.copyWithInherited(true),
              tablePolicy
            });
    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, SCHEMA))
        .thenReturn(new PolicyEntity[] {schemaPolicy, catalogPolicy.copyWithInherited(true)});
    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, CATALOG))
        .thenReturn(new PolicyEntity[] {catalogPolicy});

    PolicyEntity resolved =
        resolver
            .resolveNearest(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION)
            .orElseThrow();
    Assertions.assertEquals(11L, resolved.id());
  }

  @Test
  void testPrefersSchemaOverCatalogWhenTableHasNone() {
    PolicyEntity schemaPolicy = compactionPolicy("schema-policy", 22L).copyWithInherited(false);
    PolicyEntity catalogPolicy = compactionPolicy("catalog-policy", 33L).copyWithInherited(false);

    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(
            new PolicyEntity[] {
              schemaPolicy.copyWithInherited(true), catalogPolicy.copyWithInherited(true)
            });
    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, SCHEMA))
        .thenReturn(new PolicyEntity[] {schemaPolicy, catalogPolicy.copyWithInherited(true)});
    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, CATALOG))
        .thenReturn(new PolicyEntity[] {catalogPolicy});

    PolicyEntity resolved =
        resolver
            .resolveNearest(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION)
            .orElseThrow();
    Assertions.assertEquals(22L, resolved.id());
  }

  @Test
  void testSameLevelPicksLexicographicallySmallestName() {
    PolicyEntity zebra = compactionPolicy("zebra", 2L).copyWithInherited(false);
    PolicyEntity alpha = compactionPolicy("alpha", 1L).copyWithInherited(false);

    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(new PolicyEntity[] {zebra, alpha});

    PolicyEntity resolved =
        resolver
            .resolveNearest(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION)
            .orElseThrow();
    Assertions.assertEquals(1L, resolved.id());
    Assertions.assertEquals("alpha", resolved.name());
  }

  @Test
  void testSkipsDisabledAndMismatchedTypes() {
    PolicyEntity disabled =
        PolicyEntity.builder()
            .withId(1L)
            .withName("disabled")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withEnabled(false)
            .withContent(PolicyContents.icebergDataCompaction())
            .withAuditInfo(
                AuditInfo.builder().withCreator("u").withCreateTime(Instant.now()).build())
            .build()
            .copyWithInherited(false);
    PolicyEntity orphan =
        PolicyEntity.builder()
            .withId(2L)
            .withName("orphan")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
            .withPolicyType(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL)
            .withEnabled(true)
            .withContent(PolicyContents.icebergOrphanFileRemoval())
            .withAuditInfo(
                AuditInfo.builder().withCreator("u").withCreateTime(Instant.now()).build())
            .build()
            .copyWithInherited(false);

    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(new PolicyEntity[] {disabled, orphan});

    Assertions.assertTrue(
        resolver.resolveNearest(METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION).isEmpty());
  }

  @Test
  void testEffectivePolicyResolvesMinIntervalFromServerConfig() {
    PolicyEntity policy = compactionPolicy("p", 7L).copyWithInherited(false);
    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(new PolicyEntity[] {policy});

    Config config = new Config(false) {};
    config.set(MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS, 42_000L);

    EffectiveTableMaintenancePolicy effective =
        EffectiveTableMaintenancePolicy.resolve(
                resolver, METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION, config)
            .orElseThrow();

    Assertions.assertEquals(42_000L, effective.minIntervalMs());
    Assertions.assertNull(effective.timeTrigger());
    Assertions.assertNull(effective.commitTrigger());
    Assertions.assertEquals(7L, effective.policy().id());
  }

  @Test
  void testEffectivePolicyUsesContentExecutionFields() {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(7L)
            .withName("p")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withEnabled(true)
            .withContent(
                PolicyContents.icebergDataCompaction(
                    IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
                    IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
                    IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
                    IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
                    IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
                    IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
                    IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
                    IcebergDataCompactionContent.DEFAULT_REWRITE_OPTIONS,
                    "0 0 2 * * ?",
                    true,
                    120_000L,
                    null,
                    null))
            .withAuditInfo(
                AuditInfo.builder().withCreator("u").withCreateTime(Instant.now()).build())
            .build()
            .copyWithInherited(false);

    when(dispatcher.listPolicyInfosForMetadataObject(METALAKE, TABLE))
        .thenReturn(new PolicyEntity[] {policy});

    Config config = new Config(false) {};
    EffectiveTableMaintenancePolicy effective =
        EffectiveTableMaintenancePolicy.resolve(
                resolver, METALAKE, TABLE, Policy.BuiltInType.ICEBERG_COMPACTION, config)
            .orElseThrow();

    Assertions.assertEquals("0 0 2 * * ?", effective.timeTrigger());
    Assertions.assertEquals(Boolean.TRUE, effective.commitTrigger());
    Assertions.assertEquals(120_000L, effective.minIntervalMs());
  }

  @Test
  void testEffectivePolicyConstructorKeepsExecutionFields() {
    PolicyEntity policy = compactionPolicy("p", 7L).copyWithInherited(false);
    EffectiveTableMaintenancePolicy effective =
        new EffectiveTableMaintenancePolicy(
            policy, Policy.BuiltInType.ICEBERG_COMPACTION, "0 0 2 * * ?", true, 42_000L);
    Assertions.assertEquals("0 0 2 * * ?", effective.timeTrigger());
    Assertions.assertEquals(Boolean.TRUE, effective.commitTrigger());
    Assertions.assertEquals(42_000L, effective.minIntervalMs());
    Assertions.assertEquals(Policy.BuiltInType.ICEBERG_COMPACTION, effective.builtInType());
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
