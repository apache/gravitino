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

import java.io.IOException;
import java.util.List;
import javax.sql.DataSource;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

class TestPolicyExpandReconciler extends TestJDBCBackend {

  private static final String CRON = "0 0 2 * * ?";
  private static final String METALAKE = "metalake_for_expand_reconcile";

  @TestTemplate
  void testReconcileBackfillsExpandRowForExistingPolicy() throws IOException {
    createAndInsertMakeLake(METALAKE);

    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("nightly")
            .withNamespace(NamespaceUtil.ofPolicy(METALAKE))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withContent(compaction(CRON))
            .withEnabled(true)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    PolicyMetaService.getInstance().insertPolicy(policy, false);

    DataSource dataSource = entityStoreDataSource();
    PolicyExpandRegistrar reader = PolicyExpandRegistrar.forReads(dataSource);
    Assertions.assertTrue(reader.scheduledExecutionTime(policy.id()).isEmpty());

    PolicyExpandLifecycleHook aligner =
        new PolicyExpandLifecycleHook(PolicyExpandRegistrar.joiningCurrentTransaction());
    int aligned =
        new PolicyExpandReconciler(
                aligner, MetalakeMetaService.getInstance(), PolicyMetaService.getInstance())
            .reconcileAll();

    Assertions.assertTrue(aligned >= 1);
    Assertions.assertTrue(reader.scheduledExecutionTime(policy.id()).isPresent());
  }

  @TestTemplate
  void testReconcileSkipsPolicyWithBadCron() {
    PolicyEntity bad =
        PolicyEntity.builder()
            .withId(7L)
            .withName("bad")
            .withNamespace(NamespaceUtil.ofPolicy("ml"))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withContent(compaction("not a cron"))
            .withEnabled(true)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    BaseMetalake metalake = Mockito.mock(BaseMetalake.class);
    Mockito.when(metalake.name()).thenReturn("ml");

    MetalakeMetaService metalakes = Mockito.mock(MetalakeMetaService.class);
    PolicyMetaService policies = Mockito.mock(PolicyMetaService.class);
    Mockito.when(metalakes.listMetalakes()).thenReturn(List.of(metalake));
    Mockito.when(policies.listPoliciesByNamespace(NamespaceUtil.ofPolicy("ml")))
        .thenReturn(List.of(bad));

    PolicyExpandLifecycleHook aligner =
        new PolicyExpandLifecycleHook(PolicyExpandRegistrar.joiningCurrentTransaction());
    int aligned = new PolicyExpandReconciler(aligner, metalakes, policies).reconcileAll();

    Assertions.assertEquals(0, aligned);
  }

  private static DataSource entityStoreDataSource() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getEnvironment()
        .getDataSource();
  }

  private static PolicyContent compaction(String timeTrigger) {
    return PolicyContents.icebergDataCompaction(
        IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
        IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
        IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
        IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
        IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
        IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
        IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
        IcebergDataCompactionContent.DEFAULT_REWRITE_OPTIONS,
        timeTrigger,
        null,
        null,
        null,
        null);
  }
}
