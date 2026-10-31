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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.io.IOException;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestCommitJobClient extends TestJDBCBackend {

  @TestTemplate
  void testScheduleSkipsWhenTableMissing() {
    CommitJobClient client = new CommitJobClient(dataSource(), configWithUri());
    client.scheduleNow(NameIdentifier.of("missing-ml", "c", "s", "t"));
    Assertions.assertTrue(
        CommitJobClient.schedulerClient(dataSource())
            .getScheduledExecutionsForTask(TableMaintenanceSchedulerPools.COMMIT_JOB, Object.class)
            .isEmpty());
  }

  @TestTemplate
  void testScheduleSkipsWhenOnCommitChainEmpty() throws IOException {
    TableEntity table = insertTable("plan-ml", "iceberg", "db", "t");
    CommitJobClient client = new CommitJobClient(dataSource(), configWithUri());
    client.scheduleNow(table.nameIdentifier());
    Assertions.assertTrue(
        CommitJobClient.schedulerClient(dataSource())
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.COMMIT_JOB,
                    MaintenanceTaskIds.forCommitTable(table.id())))
            .isEmpty());
  }

  private TableEntity insertTable(String metalake, String catalog, String schema, String tableName)
      throws IOException {
    backend.insert(createBaseMakeLake(1L, metalake, AUDIT_INFO), false);
    backend.insert(createCatalog(2L, Namespace.of(metalake), catalog, AUDIT_INFO), false);
    backend.insert(
        createSchemaEntity(3L, Namespace.of(metalake, catalog), schema, AUDIT_INFO), false);
    TableEntity table =
        createTableEntity(
            101L, NamespaceUtil.ofTable(metalake, catalog, schema), tableName, AUDIT_INFO);
    backend.insert(table, false);
    return table;
  }

  private static DataSource dataSource() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getEnvironment()
        .getDataSource();
  }

  private static Config configWithUri() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_URI.getKey(),
            "http://localhost:8090"),
        t -> true);
    return config;
  }
}
