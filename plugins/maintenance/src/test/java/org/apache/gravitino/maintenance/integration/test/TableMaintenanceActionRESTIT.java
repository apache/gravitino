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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.storage.mapper.provider.MaintenanceMapperPackageProvider;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.apache.gravitino.maintenance.web.rest.feature.TableMaintenanceRESTFeature;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.mapper.provider.MapperPackageProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Boots Gravitino with {@link TableMaintenanceRESTFeature} and exercises maintenance-action
 * callback REST wiring.
 *
 * <p>Run with {@code -PskipITs} omitted (and optionally {@code -PjdbcBackend=h2|mysql|postgresql}).
 * The happy-path seeds occupancy rows via MetaService and therefore runs in embedded mode only.
 */
public class TableMaintenanceActionRESTIT extends BaseIT {

  private static final HttpClient HTTP = HttpClient.newHttpClient();
  private static final String ACCEPT = "application/vnd.gravitino.v1+json";
  private static final AuditInfo AUDIT =
      AuditInfo.builder().withCreator("it").withCreateTime(Instant.EPOCH).build();

  private static String metalake;
  private static String apiBase;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(false));
    configs.put(Configs.CACHE_ENABLED.getKey(), String.valueOf(false));
    configs.put(
        Configs.REST_API_EXTENSION_PACKAGES.getKey(),
        TableMaintenanceRESTFeature.EXTENSION_PACKAGE);
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    metalake = "ml_maint_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    client.createMetalake(metalake, "maintenance plugin IT", ImmutableMap.of());
    apiBase = serverUri + "/api";
  }

  @Test
  void testMapperPackageProviderOnClasspath() {
    long count =
        ServiceLoader.load(MapperPackageProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(MaintenanceMapperPackageProvider.class::isInstance)
            .count();
    assertEquals(1L, count);
  }

  @Test
  void testFinishUnknownActionReturnsNotFound() throws Exception {
    String body = "{\"tableId\":1,\"policyId\":1}";
    HttpResponse<String> response = postJson(actionPath(999999L, "finish"), body);
    assertEquals(404, response.statusCode(), response.body());
    assertTrue(
        response.body().contains("NotFound")
            || response.body().contains("not found")
            || response.body().toLowerCase().contains("no maintenance action")
            || response.body().contains("NoSuchEntity"),
        response.body());
  }

  @Test
  void testReportBeforeUnknownActionReturnsNotFound() throws Exception {
    String body = "{\"tableId\":1,\"policyId\":1,\"snapshotId\":10,\"metricsJson\":\"{}\"}";
    HttpResponse<String> response = postJson(actionPath(999999L, "metrics/before"), body);
    assertEquals(404, response.statusCode(), response.body());
  }

  @Test
  void testMetricsBeforeAfterFinishHappyPath() throws Exception {
    assumeTrue(
        ITUtils.EMBEDDED_TEST_MODE.equals(testMode),
        "Happy-path seeds table_maintenance_action via MetaService in the server JVM");

    long tableId = seedTableUnderMetalake(metalake);
    long policyId = RandomIdGenerator.INSTANCE.nextId();
    long jobId = 88001L;

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    OccupancyClaim claim =
        actions.insertInFlight(
            tableId, policyId, TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10));
    assertNotNull(claim);

    String beforeBody =
        "{\"tableId\":"
            + tableId
            + ",\"policyId\":"
            + policyId
            + ",\"snapshotId\":100,\"metricsJson\":\"{\\\"mse\\\":1}\"}";
    HttpResponse<String> before = postJson(actionPath(jobId, "metrics/before"), beforeBody);
    assertEquals(200, before.statusCode(), before.body());

    TableMaintenanceActionPO afterBefore =
        actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyId);
    assertNotNull(afterBefore);
    assertEquals(jobId, afterBefore.jobId());
    assertEquals(100L, afterBefore.beforeSnapshotId());
    assertEquals(0L, afterBefore.finishedAt());
    assertEquals(
        "{\"mse\":1}",
        TableSnapshotMetricsMetaService.getInstance()
            .getMetrics(tableId, policyId, 100L)
            .metricsValue());

    String afterBody =
        "{\"tableId\":"
            + tableId
            + ",\"policyId\":"
            + policyId
            + ",\"snapshotId\":200,\"metricsJson\":\"{\\\"mse\\\":2}\"}";
    HttpResponse<String> after =
        postJson(actionPath(jobId, "metrics/after") + "?finish=true", afterBody);
    assertEquals(200, after.statusCode(), after.body());

    TableMaintenanceActionPO finished =
        actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyId);
    assertNotNull(finished);
    assertEquals(200L, finished.afterSnapshotId());
    assertTrue(finished.finishedAt() > 0L);
    assertEquals(
        "{\"mse\":2}",
        TableSnapshotMetricsMetaService.getInstance()
            .getMetrics(tableId, policyId, 200L)
            .metricsValue());
  }

  private static String actionPath(long jobId, String suffix) {
    return apiBase + "/metalakes/" + metalake + "/maintenance/actions/" + jobId + "/" + suffix;
  }

  private static HttpResponse<String> postJson(String url, String body) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Accept", ACCEPT)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }

  /**
   * Inserts catalog/schema/table rows under the client-created metalake so metalake lookup succeeds
   * for callback authorization of the action's table id.
   */
  private static long seedTableUnderMetalake(String metalakeName) throws Exception {
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    String catalogName = "cat_" + UUID.randomUUID().toString().substring(0, 6);
    String schemaName = "sch";
    String tableName = "tbl";

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(catalogName)
            .withNamespace(Namespace.of(metalakeName))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("")
            .withProperties(null)
            .withAuditInfo(AUDIT)
            .build();
    store.put(catalog, false);

    SchemaEntity schema =
        SchemaEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(schemaName)
            .withNamespace(Namespace.of(metalakeName, catalogName))
            .withComment("")
            .withProperties(null)
            .withAuditInfo(AUDIT)
            .build();
    store.put(schema, false);

    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(tableName)
            .withNamespace(Namespace.of(metalakeName, catalogName, schemaName))
            .withAuditInfo(AUDIT)
            .build();
    store.put(table, false);
    return table.id();
  }
}
