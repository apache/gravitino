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

import org.apache.gravitino.integration.test.container.FixtureContainer;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.MySQLFixture;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.BaseGenericJdbcMetricsRepositoryTest;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MySQLFixture(database = TestDatabaseName.MYSQL_OPTIMIZER_JDBC_METRICS_IT)
class TestMySqlGenericJdbcMetricsRepositoryBehaviorIT extends BaseGenericJdbcMetricsRepositoryTest {

  @FixtureContainer private MySQLContainer mysql;

  @BeforeAll
  void setUp() {
    String jdbcUrl = mysql.getJdbcUrl(TestDatabaseName.MYSQL_OPTIMIZER_JDBC_METRICS_IT);
    JdbcMetricsRepositoryITUtils.initializeSchema(
        jdbcUrl, mysql.getUsername(), mysql.getPassword(), "mysql");

    storage = new GenericJdbcMetricsRepository();
    storage.initialize(
        JdbcMetricsRepositoryITUtils.createJdbcMetricsConfigs(
            jdbcUrl, mysql.getUsername(), mysql.getPassword(), "com.mysql.cj.jdbc.Driver"));
  }

  @AfterAll
  void tearDown() {
    storage.cleanupTableMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    storage.cleanupJobMetricsBefore(MAX_REASONABLE_EPOCH_SECONDS);
    storage.close();
  }
}
