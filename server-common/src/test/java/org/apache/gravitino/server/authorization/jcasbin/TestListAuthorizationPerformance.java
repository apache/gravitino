/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization.jcasbin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import jdk.jfr.Recording;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authorization.AuthorizationRequestScope;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.UserUpdatedAt;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.MockedStatic;

/** Opt-in warm-cache list benchmark; run with GRAVITINO_AUTHZ_PERF=true. */
@EnabledIfEnvironmentVariable(named = "GRAVITINO_AUTHZ_PERF", matches = "true")
public class TestListAuthorizationPerformance {
  private static final String METALAKE = "testMetalake";
  private static final Path OUTPUT =
      Path.of("/tmp/gravitino-12286-index-list-authz-performance.csv");

  /** Opens the shared persistence mocks once for the benchmark class. */
  @BeforeAll
  public static void setupBenchmark() throws Exception {
    TestJcasbinAuthorizer.setup();
    Files.write(
        Path.of("/tmp/gravitino-12286-index-perf-jvm-args.txt"),
        ManagementFactory.getRuntimeMXBean().getInputArguments());
  }

  /** Closes the persistence mocks after all benchmark methods. */
  @AfterAll
  public static void stopBenchmark() {
    TestJcasbinAuthorizer.stop();
  }

  /** Measures the real filter/index path and checks that denied objects are excluded. */
  @Test
  public void testWarmCacheListScaling() throws Exception {
    List<String> rows = new ArrayList<>();
    rows.add("roles,objects,scenario,visible,median_ms,min_ms,max_ms");
    try (Recording recording = new Recording()) {
      recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(10));
      recording.start();
      for (int roles : new int[] {10, 100, 1000}) {
        for (int objects : new int[] {100, 1000}) {
          for (String scenario :
              new String[] {
                "parent_no_deny",
                "parent_one_deny",
                "parent_outside_deny",
                "parent_many_deny",
                "object_allow",
                "rest_no_deny",
                "rest_one_deny"
              }) {
            runScenario(roles, objects, scenario, MetadataObject.Type.TABLE, rows);
            Files.write(OUTPUT, rows);
          }
        }
      }
      recording.stop();
      recording.dump(Path.of("/tmp/gravitino-12286-index-list-authz-performance.jfr"));
    }
  }

  /** Checks catalog and schema lists at the same policy scale as the table benchmark. */
  @Test
  public void testOtherListTypes() throws Exception {
    List<String> rows = new ArrayList<>();
    rows.add("roles,objects,scenario,visible,median_ms,min_ms,max_ms");
    for (MetadataObject.Type type :
        new MetadataObject.Type[] {MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA}) {
      for (String scenario :
          new String[] {"parent_no_deny", "parent_one_deny", "parent_many_deny"}) {
        runScenario(1000, scenario.equals("parent_many_deny") ? 100 : 1000, scenario, type, rows);
        Files.write(Path.of("/tmp/gravitino-12286-index-other-list-authz-performance.csv"), rows);
      }
    }
  }

  /** Separates system-wide policy population from the number of roles held by the caller. */
  @Test
  public void testGlobalPolicyPopulation() throws Exception {
    List<String> rows = new ArrayList<>();
    rows.add("roles,objects,scenario,visible,median_ms,min_ms,max_ms");
    try (Recording recording = new Recording()) {
      recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(10));
      recording.enable("jdk.JavaMonitorEnter").withThreshold(Duration.ofMillis(1));
      recording.enable("jdk.ThreadPark").withThreshold(Duration.ofMillis(2));
      recording.start();
      for (String scenario :
          new String[] {
            "global_parent_no_deny",
            "global_parent_one_deny",
            "global_parent_many_deny",
            "global_rest_no_deny",
            "global_rest_one_deny"
          }) {
        runScenario(1000, 1000, scenario, MetadataObject.Type.TABLE, rows);
        Files.write(Path.of("/tmp/gravitino-12286-index-global-list-authz-performance.csv"), rows);
      }
      recording.stop();
      recording.dump(Path.of("/tmp/gravitino-12286-index-global-list-authz-performance.jfr"));
    }
  }

  /** Repeats the high membership case without per-policy logging in a separate test invocation. */
  @Test
  public void testHighMembershipRepeat() throws Exception {
    List<String> rows = new ArrayList<>();
    rows.add("roles,objects,scenario,visible,median_ms,min_ms,max_ms");
    for (String scenario :
        new String[] {
          "parent_no_deny", "parent_one_deny", "parent_many_deny", "rest_no_deny", "rest_one_deny"
        }) {
      runScenario(1000, 100, scenario, MetadataObject.Type.TABLE, rows);
      Files.write(Path.of("/tmp/gravitino-12286-index-high-membership-repeat.csv"), rows);
    }
  }

  /** Measures a thousand listed objects with a thousand held roles and deny policies. */
  @Test
  public void testHighMembershipLargeList() throws Exception {
    List<String> rows = new ArrayList<>();
    rows.add("roles,objects,scenario,visible,median_ms,min_ms,max_ms");
    try (Recording recording = new Recording()) {
      recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(10));
      recording.enable("jdk.JavaMonitorEnter").withThreshold(Duration.ofMillis(5));
      recording.start();
      runScenario(1000, 1000, "parent_many_deny", MetadataObject.Type.TABLE, rows);
      Files.write(Path.of("/tmp/gravitino-12286-index-high-membership-large-list.csv"), rows);
      recording.stop();
      recording.dump(Path.of("/tmp/gravitino-12286-index-high-membership-large-list.jfr"));
    }
  }

  private void runScenario(
      int roles, int objects, String scenario, MetadataObject.Type type, List<String> rows)
      throws Exception {
    int heldRoles = scenario.startsWith("global_") ? 10 : roles;
    String resultScenario = scenario;
    scenario = scenario.replace("global_", "").replace("matcher_", "");
    TestJcasbinAuthorizer fixture = new TestJcasbinAuthorizer();
    when(GravitinoEnv.getInstance().config()).thenReturn(new ServerConfig());
    fixture.createAuthorizer();
    try (MockedStatic<GravitinoAuthorizerProvider> providerMock =
        mockStatic(GravitinoAuthorizerProvider.class)) {
      JcasbinAuthorizer authorizer = field(fixture, "jcasbinAuthorizer");
      // Persistence is mocked and policies remain fixed during each warm-cache run.
      JcasbinChangeListener poller = field(authorizer, "changePoller");
      poller.close();
      Config config = mock(Config.class);
      when(config.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(true);
      when(config.get(Configs.GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE)).thenReturn(100);
      when(GravitinoEnv.getInstance().config()).thenReturn(config);
      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      providerMock.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
      Map<Long, Map<PolicyKey, Effect>> indexes = new HashMap<>();
      String privilege =
          type == MetadataObject.Type.CATALOG
              ? "USE_CATALOG"
              : type == MetadataObject.Type.SCHEMA ? "USE_SCHEMA" : "SELECT_TABLE";
      NameIdentifier[] identifiers = new NameIdentifier[objects];
      List<MetadataObject> normalizedObjects = new ArrayList<>();
      warmObject(
          authorizer,
          MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
          1L,
          normalizedObjects);
      warmObject(
          authorizer,
          MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG),
          10L,
          normalizedObjects);
      warmObject(
          authorizer,
          MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA),
          11L,
          normalizedObjects);
      for (int i = 0; i < objects; i++) {
        String parent =
            type == MetadataObject.Type.CATALOG
                ? null
                : type == MetadataObject.Type.SCHEMA ? "catalog" : "catalog.schema";
        identifiers[i] =
            NameIdentifier.parse(
                METALAKE + "." + (parent == null ? "" : parent + ".") + "item" + i);
        warmObject(
            authorizer,
            MetadataObjects.of(parent, "item" + i, type),
            10000L + i,
            normalizedObjects);
      }
      for (int i = 0; i < roles; i++) {
        Map<PolicyKey, Effect> index = new HashMap<>();
        index.put(
            new PolicyKey(type.name(), 100000L + i, privilege),
            scenario.equals("parent_many_deny") ? Effect.DENY : Effect.ALLOW);
        indexes.put(100L + i, index);
      }
      Map<PolicyKey, Effect> first = indexes.get(100L);
      if (scenario.equals("object_allow")) {
        for (int i = 0; i < objects; i++) {
          first.put(new PolicyKey(type.name(), 10000L + i, privilege), Effect.ALLOW);
        }
      } else {
        String parentType =
            type == MetadataObject.Type.CATALOG
                ? "METALAKE"
                : type == MetadataObject.Type.SCHEMA ? "CATALOG" : "SCHEMA";
        long parentId =
            type == MetadataObject.Type.CATALOG
                ? 1L
                : type == MetadataObject.Type.SCHEMA ? 10L : 11L;
        first.put(new PolicyKey(parentType, parentId, privilege), Effect.ALLOW);
      }
      if (scenario.endsWith("one_deny") || scenario.equals("parent_outside_deny")) {
        first.put(
            new PolicyKey(type.name(), scenario.endsWith("one_deny") ? 10000L : 999999L, privilege),
            Effect.DENY);
      }
      if (type == MetadataObject.Type.TABLE) {
        first.put(new PolicyKey("CATALOG", 10L, "USE_CATALOG"), Effect.ALLOW);
        first.put(new PolicyKey("SCHEMA", 11L, "USE_SCHEMA"), Effect.ALLOW);
      }
      Map<Long, CachedRolePolicies> held = new HashMap<>();
      for (int i = 0; i < heldRoles; i++) {
        held.put(100L + i, new CachedRolePolicies(1L, indexes.get(100L + i)));
      }
      String expression =
          scenario.startsWith("rest_")
              ? AuthorizationExpressionConstants.LIST_TABLE_LIKE_AUTHORIZATION_EXPRESSION
              : type == MetadataObject.Type.CATALOG
                  ? AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION
                  : type == MetadataObject.Type.SCHEMA
                      ? AuthorizationExpressionConstants.FILTER_SCHEMA_AUTHORIZATION_EXPRESSION
                      : AuthorizationExpressionConstants.FILTER_TABLE_AUTHORIZATION_EXPRESSION;
      int expected = scenario.endsWith("one_deny") ? objects - 1 : objects;
      int measurements =
          Integer.parseInt(System.getenv().getOrDefault("GRAVITINO_AUTHZ_PERF_MEASUREMENTS", "3"));
      int warmups =
          Integer.parseInt(System.getenv().getOrDefault("GRAVITINO_AUTHZ_PERF_WARMUPS", "1"));
      double[] times = new double[measurements];
      for (int iteration = -warmups; iteration < times.length; iteration++) {
        AuthorizationRequestContext context = new AuthorizationRequestContext();
        context.computeUserInfoIfAbsent(
            JcasbinAuthorizationCacheKeys.userRoleKey(METALAKE, "tester"),
            k -> Optional.of(new UserUpdatedAt(2L, 1000L)));
        context.setPrefetchedRoleVersions(Collections.emptyMap());
        // Canonical names are prevalidated on the caller thread; worker threads must not depend
        // on Mockito's thread-local catalog mocks. Both benchmark variants use this same setup.
        for (MetadataObject object : normalizedObjects) {
          context.computeNormalizedMetadataObjectIfAbsent(
              JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, object), key -> object);
        }
        // Storage/version probes are excluded; request index construction is included in timing.
        context.setBoundRoleIds(new ArrayList<>(held.keySet()));
        try (AuthorizationRequestScope scope = AuthorizationRequestScope.open()) {
          scope.bind(METALAKE, context);
          long start = System.nanoTime();
          context.loadRole(
              () ->
                  context.setRolePolicyView(
                      new RequestRolePolicies(
                          0L, held, ActiveRoles.all(), Collections.emptyMap())));
          NameIdentifier[] visible =
              MetadataAuthzHelper.filterByExpression(
                  METALAKE, expression, Entity.EntityType.valueOf(type.name()), identifiers);
          double elapsed = (System.nanoTime() - start) / 1_000_000.0;
          assertEquals(expected, visible.length, scenario);
          List<NameIdentifier> expectedIds = new ArrayList<>(Arrays.asList(identifiers));
          if (scenario.endsWith("one_deny")) {
            expectedIds.remove(0);
          }
          assertEquals(
              new HashSet<>(expectedIds),
              new HashSet<>(Arrays.asList(visible)),
              "The returned object set must match every expected object");
          if (scenario.endsWith("one_deny")) {
            assertFalse(
                Arrays.asList(visible).contains(identifiers[0]),
                "The denied object must be excluded");
          }
          if (iteration >= 0) {
            times[iteration] = elapsed;
          }
        }
      }
      Arrays.sort(times);
      rows.add(
          String.format(
              Locale.ROOT,
              "%d,%d,%s,%d,%.3f,%.3f,%.3f",
              roles,
              objects,
              type.name() + "_" + resultScenario,
              expected,
              times[times.length / 2],
              times[0],
              times[times.length - 1]));
    } finally {
      fixture.closeAuthorizer();
    }
  }

  private static void warmObject(
      JcasbinAuthorizer authorizer,
      MetadataObject object,
      long id,
      List<MetadataObject> normalizedObjects)
      throws Exception {
    GravitinoCache<String, Long> ids = field(authorizer, "metadataIdCache");
    GravitinoCache<Long, Optional<OwnerInfo>> owners = field(authorizer, "ownerRelCache");
    normalizedObjects.add(object);
    ids.put(JcasbinAuthorizationCacheKeys.metadataIdCacheKey(METALAKE, object), id);
    owners.put(id, Optional.empty());
  }

  @SuppressWarnings("unchecked")
  private static <T> T field(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(target);
  }
}
