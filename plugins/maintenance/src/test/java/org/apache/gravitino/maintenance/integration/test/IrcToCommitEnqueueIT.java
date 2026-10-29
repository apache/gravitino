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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.listener.api.event.IcebergUpdateTableEvent;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.integration.test.MaintenanceSchedulerITSupport.SeededTable;
import org.apache.gravitino.maintenance.listener.PostCommitMaintenanceListener;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.SchedulerPoolTestHooks;
import org.apache.gravitino.maintenance.web.rest.feature.TableMaintenanceRESTFeature;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Boots Gravitino with {@link TableMaintenanceRESTFeature} and {@link
 * PostCommitMaintenanceListener}, then dispatches an IRC table-update event and asserts commit-job
 * enqueue.
 *
 * <p>Embedded mode only (same-JVM EventBus / MetaService). Run with {@code -PskipITs} omitted.
 */
public class IrcToCommitEnqueueIT extends BaseIT {

  private static final String LISTENER_NAME = "tmsIrc";

  private static final AtomicReference<String> CAPTURED_COMMIT_ID = new AtomicReference<>();
  private static final AtomicReference<MaintenanceJobSpec> CAPTURED_COMMIT_SPEC =
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
    configs.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX
            + EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES,
        LISTENER_NAME);
    configs.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX
            + LISTENER_NAME
            + "."
            + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        PostCommitMaintenanceListener.class.getName());
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    SchedulerPoolTestHooks.installCommitSubmit(
        (instanceId, spec) -> {
          CAPTURED_COMMIT_ID.set(instanceId);
          CAPTURED_COMMIT_SPEC.set(spec);
        });

    metalake = "ml_irc_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    client.createMetalake(metalake, "IRC→commit IT", ImmutableMap.of());
  }

  @Test
  void testIrcUpdateEventEnqueuesCommitJob() throws Exception {
    assumeTrue(
        ITUtils.EMBEDDED_TEST_MODE.equals(testMode),
        "Asserts commit submit capture in the same JVM as the EventBus listener");

    CAPTURED_COMMIT_ID.set(null);
    CAPTURED_COMMIT_SPEC.set(null);

    SeededTable seeded =
        MaintenanceSchedulerITSupport.seedTaggedCompactionTable(
            client.loadMetalake(metalake),
            metalake,
            /*timeTrigger=*/ null,
            /*commitTrigger=*/ true);

    NameIdentifier tableId =
        NameIdentifier.of(metalake, seeded.catalogName, seeded.schemaName, seeded.tableName);
    GravitinoEnv.getInstance().eventBus().dispatchEvent(icebergUpdateTableEvent(tableId));

    awaitCommitCapture(MaintenanceTaskIds.forCommitTable(seeded.tableId), 30L);

    MaintenanceJobSpec spec = CAPTURED_COMMIT_SPEC.get();
    assertNotNull(spec);
    assertTrue(spec.isValid());
    assertEquals(List.of(seeded.tableId), spec.tableIds());
    assertEquals(seeded.policyId, spec.getTasks().get(0).getPolicyId());
    assertTrue(spec.getUpdaterOptions() != null && !spec.getUpdaterOptions().isBlank());
  }

  private static IcebergUpdateTableEvent icebergUpdateTableEvent(NameIdentifier tableId) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    Enumeration<String> headerNames = Collections.enumeration(Collections.emptyList());
    when(request.getRemoteHost()).thenReturn("localhost");
    when(request.getHeaderNames()).thenReturn(headerNames);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), "file:///tmp/tms-irc-it", ImmutableMap.of());
    return new IcebergUpdateTableEvent(
        new IcebergRequestContext(request, tableId.namespace().level(1)),
        tableId,
        new UpdateTableRequest(Collections.emptyList(), Collections.emptyList()),
        LoadTableResponse.builder().withTableMetadata(metadata).build());
  }

  private static void awaitCommitCapture(String expectedInstanceId, long timeoutSeconds)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    while (System.nanoTime() < deadline) {
      if (expectedInstanceId.equals(CAPTURED_COMMIT_ID.get())
          && CAPTURED_COMMIT_SPEC.get() != null) {
        return;
      }
      Thread.sleep(50L);
    }
    assertEquals(
        expectedInstanceId,
        CAPTURED_COMMIT_ID.get(),
        "IRC listener did not enqueue expected commit-job within " + timeoutSeconds + "s");
  }
}
