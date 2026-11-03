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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import java.time.Instant;
import javax.sql.DataSource;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestPolicyExpandClient extends TestJDBCBackend {

  private static final String CRON = "0 0 2 * * ?";
  private static final String LATER_CRON = "0 30 4 * * ?";

  @TestTemplate
  void testScheduleRescheduleAndRemoveTheExpandRow() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandClient writer = PolicyExpandClient.joiningCurrentTransaction();
    PolicyExpandClient reader = PolicyExpandClient.forReads(dataSource);

    SessionUtils.doMultipleWithCommit(() -> schedule(writer, 42L, CRON));
    Instant firstDue = executionTime(reader, 42L);
    Assertions.assertTrue(firstDue.isAfter(Instant.now()));

    SessionUtils.doMultipleWithCommit(() -> schedule(writer, 42L, LATER_CRON));
    Instant secondDue = executionTime(reader, 42L);
    Assertions.assertNotEquals(firstDue, secondDue);

    SessionUtils.doMultipleWithCommit(() -> writer.remove(42L));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isEmpty());

    SessionUtils.doMultipleWithCommit(() -> schedule(writer, 42L, CRON));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isPresent());
    SessionUtils.doMultipleWithCommit(() -> writer.remove(42L));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isEmpty());
    SessionUtils.doMultipleWithCommit(() -> writer.remove(42L));
  }

  @TestTemplate
  void testFailedTransactionRollsBackTheExpandRow() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandClient writer = PolicyExpandClient.joiningCurrentTransaction();
    PolicyExpandClient reader = PolicyExpandClient.forReads(dataSource);

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> schedule(writer, 99L, CRON),
                () -> {
                  throw new IllegalStateException("boom");
                }));

    Assertions.assertTrue(reader.scheduledExecutionTime(99L).isEmpty());
  }

  @TestTemplate
  void testInvalidCronFailsBeforeAnExpandRowIsWritten() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandClient writer = PolicyExpandClient.joiningCurrentTransaction();
    PolicyExpandClient reader = PolicyExpandClient.forReads(dataSource);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SessionUtils.doMultipleWithCommit(() -> schedule(writer, 100L, "not a cron")));

    Assertions.assertTrue(reader.scheduledExecutionTime(100L).isEmpty());
  }

  private static DataSource entityStoreDataSource() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getEnvironment()
        .getDataSource();
  }

  private static void schedule(PolicyExpandClient writer, long policyId, String cron) {
    writer.scheduleAt(policyId, PolicyExpandClient.nextExecution(cron));
  }

  private static Instant executionTime(PolicyExpandClient expandClient, long policyId) {
    return expandClient
        .scheduledExecutionTime(policyId)
        .orElseThrow(() -> new AssertionError("missing expand row " + policyId));
  }
}
