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

package org.apache.gravitino.maintenance.optimizer.integration.test.storage;

import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.BaseGenericJdbcMetricsRepositoryTest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestPostgreSqlGenericJdbcMetricsRepositoryIT extends BaseGenericJdbcMetricsRepositoryTest {

  private PostgreSQLContainer<?> postgres;

  @BeforeAll
  void setUp() {
    postgres = new PostgreSQLContainer<>("postgres:13");
    postgres.start();
    JdbcMetricsRepositoryITUtils.initializeSchema(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), "postgresql");

    storage = new GenericJdbcMetricsRepository();
    storage.initialize(
        JdbcMetricsRepositoryITUtils.createJdbcMetricsConfigs(
            postgres.getJdbcUrl(),
            postgres.getUsername(),
            postgres.getPassword(),
            postgres.getDriverClassName()));
  }

  @AfterAll
  void tearDown() {
    storage.cleanupTableMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    storage.cleanupJobMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    storage.close();
    if (postgres != null) {
      postgres.stop();
    }
  }
}
