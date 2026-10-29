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
package org.apache.gravitino.maintenance.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.apache.gravitino.Configs;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.integration.test.MaintenanceSchedulerITSupport.SeededTable;
import org.apache.gravitino.maintenance.occupancy.OccupancyGate;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.policy.PolicyExpandRegistrar;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.SchedulerPoolTestHooks;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandTableCandidate;
import org.apache.gravitino.maintenance.scheduler.policy.expand.NearestPolicyTableEnumerator;
import org.apache.gravitino.maintenance.web.rest.feature.TableMaintenanceRESTFeature;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Boots Gravitino with {@link TableMaintenanceRESTFeature} and exercises policy-expand → cron-job
 * enqueue through the live expand pool and nearest-policy table walk.
 *
 * <p>Embedded mode only (same-JVM MetaService / SchedulerClient). Run with {@code -PskipITs}
 * omitted.
 */
public class PolicyExpandToCronIT extends BaseIT {

  private static final String TIME_TRIGGER = "0 0 2 * * ?";

  private static final AtomicReference<String> CAPTURED_CRON_ID = new AtomicReference<>();
  private static final AtomicReference<MaintenanceJobSpec> CAPTURED_CRON_SPEC =
      new AtomicReference<>();

  private static String metalake;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(false));
    configs.put(Configs.CACHE_ENABLED.getKey(), String.valueOf(false));
    configs.put(
        Configs.REST_API_EXTENSION_PACKAGES.getKey(),
        TableMaintenanceRESTFeature.EXTENSION_PACKAGE);
    configs.put(TableMaintenanceConfig.SCHEDULER_POLLING_INTERVAL_MS.getKey(), "100");
    configs.put(
        MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_URI.getKey(),
        "http://127.0.0.1:8090");
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    SchedulerPoolTestHooks.installCronSubmit(
        (instanceId, spec) -> {
          CAPTURED_CRON_ID.set(instanceId);
          CAPTURED_CRON_SPEC.set(spec);
        });

    metalake = "ml_expand_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    client.createMetalake(metalake, "expand→cron IT", ImmutableMap.of());
  }

  @Test
  void testExpandPoolEnqueuesCronJobForTaggedTable() throws Exception {
    assumeTrue(
        ITUtils.EMBEDDED_TEST_MODE.equals(testMode),
        "Asserts cron submit capture in the same JVM as the expand pool");

    CAPTURED_CRON_ID.set(null);
    CAPTURED_CRON_SPEC.set(null);

    SeededTable seeded =
        MaintenanceSchedulerITSupport.seedTaggedCompactionTable(
            client.loadMetalake(metalake), metalake, TIME_TRIGGER, /*commitTrigger=*/ null);

    PolicyEntity policy = PolicyMetaService.getInstance().getPolicyById(seeded.policyId);
    List<ExpandTableCandidate> candidates =
        new NearestPolicyTableEnumerator()
            .listTableIds(policy, 10, new OccupancyGate()::shouldSkip, 0L);
    assertEquals(
        List.of(seeded.tableId),
        candidates.stream().map(ExpandTableCandidate::tableId).toList(),
        "nearest enumerator should see the tagged Iceberg table");

    DataSource ds = dataSource();
    assertTrue(
        PolicyExpandRegistrar.forReads(ds).scheduledExecutionTime(seeded.policyId).isPresent(),
        "createPolicy should register a policy-expand row via PolicyExpandLifecycleHook");

    // Force the expand lease overdue so the live expand pool picks it (production next fire is
    // tomorrow 02:00 for TIME_TRIGGER).
    PolicyExpandRegistrar.forReads(ds).scheduleAt(seeded.policyId, Instant.now().minusSeconds(5));

    awaitCronCapture(MaintenanceTaskIds.forCronTable(seeded.tableId, seeded.policyId), 30L);

    MaintenanceJobSpec spec = CAPTURED_CRON_SPEC.get();
    assertNotNull(spec);
    assertTrue(spec.isValid());
    assertEquals(List.of(seeded.tableId), spec.tableIds());
    assertEquals(seeded.policyId, spec.getOperations().get(0).getPolicyId());
    assertTrue(spec.getJobConf().containsKey("updater_options"));
    assertEquals(seeded.catalogName, spec.getTables().get(0).catalogName());
    assertEquals(
        seeded.schemaName + "." + seeded.tableName, spec.getTables().get(0).sparkTableIdentifier());
  }

  private static void awaitCronCapture(String expectedInstanceId, long timeoutSeconds)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    while (System.nanoTime() < deadline) {
      if (expectedInstanceId.equals(CAPTURED_CRON_ID.get()) && CAPTURED_CRON_SPEC.get() != null) {
        return;
      }
      Thread.sleep(50L);
    }
    assertEquals(
        expectedInstanceId,
        CAPTURED_CRON_ID.get(),
        "expand pool did not enqueue expected cron-job within " + timeoutSeconds + "s");
  }

  private static DataSource dataSource() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getEnvironment()
        .getDataSource();
  }
}
