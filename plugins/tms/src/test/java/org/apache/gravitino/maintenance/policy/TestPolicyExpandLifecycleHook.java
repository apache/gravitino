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

import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestPolicyExpandLifecycleHook extends TestJDBCBackend {

  private static final String CRON = "0 0 2 * * ?";

  @TestTemplate
  void testEnabledMaintenancePolicyIsScheduled() {
    PolicyExpandRowWriter writer = PolicyExpandRowWriter.joiningCurrentTransaction();
    PolicyExpandRowWriter reader = PolicyExpandRowWriter.forReads(entityStoreDataSource());
    PolicyExpandLifecycleHook hook = new PolicyExpandLifecycleHook(writer);

    SessionUtils.doMultipleWithCommit(() -> hook.afterPersist(policy(42L, true, compaction(CRON))));

    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isPresent());
  }

  @TestTemplate
  void testDisabledMaintenancePolicyRemovesTheRow() {
    PolicyExpandRowWriter writer = PolicyExpandRowWriter.joiningCurrentTransaction();
    PolicyExpandRowWriter reader = PolicyExpandRowWriter.forReads(entityStoreDataSource());
    PolicyExpandLifecycleHook hook = new PolicyExpandLifecycleHook(writer);

    SessionUtils.doMultipleWithCommit(() -> hook.afterPersist(policy(7L, true, compaction(CRON))));
    Assertions.assertTrue(reader.scheduledExecutionTime(7L).isPresent());

    SessionUtils.doMultipleWithCommit(() -> hook.afterUpdate(policy(7L, false, compaction(CRON))));
    Assertions.assertTrue(reader.scheduledExecutionTime(7L).isEmpty());
  }

  @TestTemplate
  void testNonMaintenancePolicyRemovesAnyStrayRow() {
    PolicyExpandRowWriter writer = PolicyExpandRowWriter.joiningCurrentTransaction();
    PolicyExpandRowWriter reader = PolicyExpandRowWriter.forReads(entityStoreDataSource());
    PolicyExpandLifecycleHook hook = new PolicyExpandLifecycleHook(writer);
    PolicyContent custom = PolicyContents.custom(Map.of(), Set.of(MetadataObject.Type.TABLE), null);

    SessionUtils.doMultipleWithCommit(() -> writer.sync(1L, true, CRON));
    Assertions.assertTrue(reader.scheduledExecutionTime(1L).isPresent());

    SessionUtils.doMultipleWithCommit(
        () -> hook.afterPersist(policy(1L, true, custom, Policy.BuiltInType.CUSTOM)));
    Assertions.assertTrue(reader.scheduledExecutionTime(1L).isEmpty());
  }

  @TestTemplate
  void testDeleteRemovesByPolicyId() {
    PolicyExpandRowWriter writer = PolicyExpandRowWriter.joiningCurrentTransaction();
    PolicyExpandRowWriter reader = PolicyExpandRowWriter.forReads(entityStoreDataSource());
    PolicyExpandLifecycleHook hook = new PolicyExpandLifecycleHook(writer);

    SessionUtils.doMultipleWithCommit(() -> writer.sync(11L, true, CRON));
    Assertions.assertTrue(reader.scheduledExecutionTime(11L).isPresent());

    SessionUtils.doMultipleWithCommit(() -> hook.beforeRemove(11L));
    Assertions.assertTrue(reader.scheduledExecutionTime(11L).isEmpty());
  }

  private static DataSource entityStoreDataSource() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getEnvironment()
        .getDataSource();
  }

  private static PolicyEntity policy(long id, boolean enabled, PolicyContent content) {
    return policy(id, enabled, content, Policy.BuiltInType.ICEBERG_COMPACTION);
  }

  private static PolicyEntity policy(
      long id, boolean enabled, PolicyContent content, Policy.BuiltInType type) {
    return PolicyEntity.builder()
        .withId(id)
        .withName("nightly")
        .withNamespace(NamespaceUtil.ofPolicy("ml"))
        .withPolicyType(type)
        .withContent(content)
        .withEnabled(enabled)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
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
