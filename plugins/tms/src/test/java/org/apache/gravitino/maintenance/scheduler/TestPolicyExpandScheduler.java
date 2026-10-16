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
package org.apache.gravitino.maintenance.scheduler;

import java.time.Instant;
import javax.sql.DataSource;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestPolicyExpandScheduler extends TestJDBCBackend {

  private static final String CRON = "0 0 2 * * ?";
  private static final String LATER_CRON = "0 30 4 * * ?";

  @TestTemplate
  void testSyncInsertsReschedulesAndRemovesTheExpandRow() {
    DataSource dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    PolicyExpandScheduler scheduler = PolicyExpandScheduler.create(dataSource);

    scheduler.sync(42L, true, CRON);
    Instant firstDue = executionTime(scheduler, 42L);
    Assertions.assertTrue(firstDue.isAfter(Instant.now()));

    scheduler.sync(42L, true, LATER_CRON);
    Instant secondDue = executionTime(scheduler, 42L);
    Assertions.assertNotEquals(firstDue, secondDue);

    scheduler.sync(42L, false, LATER_CRON);
    Assertions.assertTrue(scheduler.scheduledExecutionTime(42L).isEmpty());

    scheduler.sync(42L, true, CRON);
    Assertions.assertTrue(scheduler.scheduledExecutionTime(42L).isPresent());
    scheduler.remove(42L);
    Assertions.assertTrue(scheduler.scheduledExecutionTime(42L).isEmpty());
    scheduler.remove(42L);
  }

  private static Instant executionTime(PolicyExpandScheduler scheduler, long policyId) {
    return scheduler
        .scheduledExecutionTime(policyId)
        .orElseThrow(() -> new AssertionError("missing expand row " + policyId));
  }
}
