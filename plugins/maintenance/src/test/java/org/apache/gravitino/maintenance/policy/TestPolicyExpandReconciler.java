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

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.ExecutionOperations;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

class TestPolicyExpandReconciler extends TestJDBCBackend {

  private static final String CRON = "0 0 2 * * ?";
  private static final String LATER_CRON = "0 30 4 * * ?";
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

    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    PolicyExpandLifecycleHook aligner = new PolicyExpandLifecycleHook(writer);
    int aligned =
        new PolicyExpandReconciler(
                aligner, writer, MetalakeMetaService.getInstance(), PolicyMetaService.getInstance())
            .reconcileAll();

    Assertions.assertTrue(aligned >= 1);
    Assertions.assertTrue(reader.scheduledExecutionTime(policy.id()).isPresent());
  }

  @TestTemplate
  void testReconcilePreservesOverdueExpandDue() throws IOException {
    createAndInsertMakeLake(METALAKE + "_overdue");
    String metalake = METALAKE + "_overdue";
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("overdue")
            .withNamespace(NamespaceUtil.ofPolicy(metalake))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withContent(compaction(CRON))
            .withEnabled(true)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    PolicyMetaService.getInstance().insertPolicy(policy, false);

    // db-scheduler / JDBC timestamps are microsecond precision; truncate so round-trip equals.
    Instant overdue = Instant.now().minusSeconds(3600).truncatedTo(ChronoUnit.MICROS);
    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    PolicyExpandRegistrar reader = PolicyExpandRegistrar.forReads(entityStoreDataSource());
    SessionUtils.doMultipleWithCommit(() -> writer.scheduleAt(policy.id(), overdue));
    Assertions.assertEquals(overdue, reader.scheduledExecutionTime(policy.id()).orElseThrow());

    PolicyExpandLifecycleHook aligner = new PolicyExpandLifecycleHook(writer);
    new PolicyExpandReconciler(
            aligner, writer, MetalakeMetaService.getInstance(), PolicyMetaService.getInstance())
        .reconcileAll();

    Assertions.assertEquals(overdue, reader.scheduledExecutionTime(policy.id()).orElseThrow());
  }

  @TestTemplate
  void testReconcileCorrectsStaleExpandDueTime() throws IOException {
    createAndInsertMakeLake(METALAKE + "_stale");
    String metalake = METALAKE + "_stale";
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("stale-due")
            .withNamespace(NamespaceUtil.ofPolicy(metalake))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withContent(compaction(CRON))
            .withEnabled(true)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    PolicyMetaService.getInstance().insertPolicy(policy, false);

    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    PolicyExpandRegistrar reader = PolicyExpandRegistrar.forReads(entityStoreDataSource());
    SessionUtils.doMultipleWithCommit(() -> writer.sync(policy.id(), true, LATER_CRON));
    Instant staleDue = reader.scheduledExecutionTime(policy.id()).orElseThrow();

    PolicyExpandLifecycleHook aligner = new PolicyExpandLifecycleHook(writer);
    new PolicyExpandReconciler(
            aligner, writer, MetalakeMetaService.getInstance(), PolicyMetaService.getInstance())
        .reconcileAll();

    Instant corrected = reader.scheduledExecutionTime(policy.id()).orElseThrow();
    Assertions.assertNotEquals(staleDue, corrected);
  }

  @TestTemplate
  void testReconcileRemovesExpandRowForInvalidCron() {
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

    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    SessionUtils.doMultipleWithCommit(() -> writer.sync(7L, true, CRON));
    PolicyExpandRegistrar reader = PolicyExpandRegistrar.forReads(entityStoreDataSource());
    Assertions.assertTrue(reader.scheduledExecutionTime(7L).isPresent());

    PolicyExpandLifecycleHook aligner = new PolicyExpandLifecycleHook(writer);
    Assertions.assertDoesNotThrow(
        () -> new PolicyExpandReconciler(aligner, writer, metalakes, policies).reconcileAll());
    Assertions.assertTrue(reader.scheduledExecutionTime(7L).isEmpty());
  }

  @TestTemplate
  void testReconcileRemovesOrphanExpandRow() {
    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    SessionUtils.doMultipleWithCommit(() -> writer.sync(99L, true, CRON));
    PolicyExpandRegistrar reader = PolicyExpandRegistrar.forReads(entityStoreDataSource());
    Assertions.assertTrue(reader.scheduledExecutionTime(99L).isPresent());

    MetalakeMetaService metalakes = Mockito.mock(MetalakeMetaService.class);
    PolicyMetaService policies = Mockito.mock(PolicyMetaService.class);
    Mockito.when(metalakes.listMetalakes()).thenReturn(List.of());

    PolicyExpandLifecycleHook aligner = new PolicyExpandLifecycleHook(writer);
    new PolicyExpandReconciler(aligner, writer, metalakes, policies).reconcileAll();
    Assertions.assertTrue(reader.scheduledExecutionTime(99L).isEmpty());
  }

  @TestTemplate
  void testCompletePickReschedulesEnabledPolicy() throws IOException {
    createAndInsertMakeLake(METALAKE + "_pick");
    String metalake = METALAKE + "_pick";
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("picked")
            .withNamespace(NamespaceUtil.ofPolicy(metalake))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withContent(compaction(CRON))
            .withEnabled(true)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    PolicyMetaService.getInstance().insertPolicy(policy, false);

    ExecutionOperations<Void> operations = Mockito.mock(ExecutionOperations.class);
    ExecutionComplete complete = ExecutionComplete.simulatedSuccess(Instant.now());
    PolicyExpandLifecycleHook.completePick(Long.toString(policy.id()), complete, operations);

    Mockito.verify(operations)
        .reschedule(Mockito.eq(complete), Mockito.argThat(due -> due.isAfter(Instant.now())));
    Mockito.verify(operations, Mockito.never()).remove();
  }

  @TestTemplate
  void testCompletePickRemovesMissingPolicy() {
    ExecutionOperations<Void> operations = Mockito.mock(ExecutionOperations.class);
    ExecutionComplete complete = ExecutionComplete.simulatedSuccess(Instant.now());
    PolicyExpandLifecycleHook.completePick("424242", complete, operations);
    Mockito.verify(operations).remove();
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
