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

import java.time.Instant;
import javax.sql.DataSource;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestPolicyExpandRowWriter extends TestJDBCBackend {

  private static final String CRON = "0 0 2 * * ?";
  private static final String LATER_CRON = "0 30 4 * * ?";

  @TestTemplate
  void testSyncInsertsReschedulesAndRemovesTheExpandRow() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandRowWriter writer = PolicyExpandRowWriter.joiningCurrentTransaction();
    PolicyExpandRowWriter reader = PolicyExpandRowWriter.forReads(dataSource);

    SessionUtils.doMultipleWithCommit(() -> writer.sync(42L, true, CRON));
    Instant firstDue = executionTime(reader, 42L);
    Assertions.assertTrue(firstDue.isAfter(Instant.now()));

    SessionUtils.doMultipleWithCommit(() -> writer.sync(42L, true, LATER_CRON));
    Instant secondDue = executionTime(reader, 42L);
    Assertions.assertNotEquals(firstDue, secondDue);

    SessionUtils.doMultipleWithCommit(() -> writer.sync(42L, false, LATER_CRON));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isEmpty());

    SessionUtils.doMultipleWithCommit(() -> writer.sync(42L, true, CRON));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isPresent());
    SessionUtils.doMultipleWithCommit(() -> writer.remove(42L));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isEmpty());
    SessionUtils.doMultipleWithCommit(() -> writer.remove(42L));
  }

  @TestTemplate
  void testFailedTransactionRollsBackTheExpandRow() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandRowWriter writer = PolicyExpandRowWriter.joiningCurrentTransaction();
    PolicyExpandRowWriter reader = PolicyExpandRowWriter.forReads(dataSource);

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> writer.sync(99L, true, CRON),
                () -> {
                  throw new IllegalStateException("boom");
                }));

    Assertions.assertTrue(reader.scheduledExecutionTime(99L).isEmpty());
  }

  @TestTemplate
  void testInvalidCronFailsBeforeAnExpandRowIsWritten() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandRowWriter writer = PolicyExpandRowWriter.joiningCurrentTransaction();
    PolicyExpandRowWriter reader = PolicyExpandRowWriter.forReads(dataSource);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SessionUtils.doMultipleWithCommit(() -> writer.sync(100L, true, "not a cron")));

    Assertions.assertTrue(reader.scheduledExecutionTime(100L).isEmpty());
  }

  private static DataSource entityStoreDataSource() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getEnvironment()
        .getDataSource();
  }

  private static Instant executionTime(PolicyExpandRowWriter rowWriter, long policyId) {
    return rowWriter
        .scheduledExecutionTime(policyId)
        .orElseThrow(() -> new AssertionError("missing expand row " + policyId));
  }
}
