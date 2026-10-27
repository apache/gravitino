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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.MockedStatic;

/** Tests metric identity rules, atomic writes, and cleanup on every relational backend. */
public class TestSemanticModelMetricIdentities extends TestJDBCBackend {

  @TestTemplate
  void testExactNamesContentChangesAndReintroduction() throws Exception {
    Namespace namespace = parents();
    String longName = "metric_" + "x".repeat(300);
    SemanticModelEntity original = model(namespace, "sales", "Revenue", "revenue", "收入'", longName);
    backend.insert(original, false);
    Map<String, Long> initial = liveIds(original.id());
    assertEquals(Set.of("Revenue", "revenue", "收入'", longName), initial.keySet());
    assertEquals(4, Set.copyOf(initial.values()).size());
    SemanticModelPO stored = service().getSemanticModelPOByIdentifier(original.nameIdentifier());
    for (SemanticModelMetricPO metric : live(original.id())) {
      assertEquals(stored.getMetalakeId(), metric.metalakeId());
      assertEquals(stored.getCatalogId(), metric.catalogId());
      assertEquals(stored.getSchemaId(), metric.schemaId());
      assertEquals(original.id(), metric.semanticModelId());
      assertEquals(0L, metric.deletedAt());
    }

    Metric[] reordered =
        Arrays.stream(new String[] {longName, "收入'", "revenue", "Revenue"})
            .map(
                name ->
                    Metric.builder()
                        .withName(name)
                        .withExpression(expression("SUM(orders.amount) * 2"))
                        .withDescription("Updated description")
                        .build())
            .toArray(Metric[]::new);
    SemanticModelEntity renamed = copy(original, original.id(), "renamed", reordered);
    backend.update(original.nameIdentifier(), original.type(), old -> renamed);
    assertEquals(initial, liveIds(original.id()));
    assertEquals(4, count("semantic_model_metric_meta", original.id()));

    SemanticModelEntity reduced = copy(renamed, renamed.id(), renamed.name(), metrics("Revenue"));
    backend.update(renamed.nameIdentifier(), renamed.type(), old -> reduced);
    assertEquals(Map.of("Revenue", initial.get("Revenue")), liveIds(original.id()));
    SemanticModelEntity reintroduced =
        copy(reduced, reduced.id(), reduced.name(), metrics("Revenue", "revenue"));
    backend.update(reduced.nameIdentifier(), reduced.type(), old -> reintroduced);
    Map<String, Long> current = liveIds(original.id());
    assertEquals(initial.get("Revenue"), current.get("Revenue"));
    assertNotEquals(initial.get("revenue"), current.get("revenue"));
    assertEquals(5, count("semantic_model_metric_meta", original.id()));
    assertSnapshotNames(reintroduced);
  }

  @TestTemplate
  void testWhitespaceOnlyNamesOnCreateReplaceAndOverwrite() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity original = model(namespace, "sales", " ");
    backend.insert(original, false);
    Map<String, Long> initial = liveIds(original.id());
    assertEquals(Set.of(" "), initial.keySet());
    assertSnapshotNames(backend.get(original.nameIdentifier(), original.type()));

    SemanticModelEntity replacement =
        copy(original, original.id(), original.name(), metrics(" ", "\t"));
    backend.update(original.nameIdentifier(), original.type(), old -> replacement);
    Map<String, Long> replaced = liveIds(original.id());
    assertEquals(Set.of(" ", "\t"), replaced.keySet());
    assertEquals(initial.get(" "), replaced.get(" "));
    assertSnapshotNames(backend.get(original.nameIdentifier(), original.type()));

    SemanticModelEntity imported = model(namespace, original.name(), " ", "\t", "\n");
    backend.insert(imported, true);
    Map<String, Long> overwritten = liveIds(original.id());
    assertEquals(Set.of(" ", "\t", "\n"), overwritten.keySet());
    assertEquals(replaced.get(" "), overwritten.get(" "));
    assertEquals(replaced.get("\t"), overwritten.get("\t"));
    assertEquals(3, Set.copyOf(overwritten.values()).size());
    assertEquals(0, count("semantic_model_metric_meta", imported.id()));
    SemanticModelEntity loaded = backend.get(original.nameIdentifier(), original.type());
    assertEquals(original.id(), loaded.id());
    assertSnapshotNames(loaded);
  }

  @TestTemplate
  void testAbsentEmptyAndModelScopedNames() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity absent = model(namespace, "absent", (String[]) null);
    SemanticModelEntity empty = model(namespace, "empty");
    backend.insert(absent, false);
    backend.insert(empty, true);
    assertEquals(0, count("semantic_model_metric_meta", absent.id()));
    assertEquals(0, count("semantic_model_metric_meta", empty.id()));
    SemanticModelEntity first = model(namespace, "first", "revenue");
    SemanticModelEntity second = model(namespace, "second", "revenue");
    backend.insert(first, false);
    backend.insert(second, false);
    assertNotEquals(liveIds(first.id()).get("revenue"), liveIds(second.id()).get("revenue"));
    SemanticModelEntity noMetrics = copy(first, first.id(), first.name(), null);
    backend.update(first.nameIdentifier(), first.type(), old -> noMetrics);
    assertTrue(liveIds(first.id()).isEmpty());
    assertEquals(Set.of("revenue"), liveIds(second.id()).keySet());
  }

  @TestTemplate
  void testOverwriteUsesPersistedModelAndMetricIds() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity original = model(namespace, "sales", "kept", "removed");
    backend.insert(original, true);
    Map<String, Long> initial = liveIds(original.id());
    SemanticModelEntity imported = model(namespace, original.name(), "kept", "added");
    backend.insert(imported, true);
    assertEquals(Set.of("kept", "added"), liveIds(original.id()).keySet());
    assertEquals(initial.get("kept"), liveIds(original.id()).get("kept"));
    assertEquals(0, count("semantic_model_metric_meta", imported.id()));
    assertEquals(
        2, service().getSemanticModelPOByIdentifier(original.nameIdentifier()).getCurrentVersion());
    SemanticModelEntity loaded = backend.get(original.nameIdentifier(), original.type());
    assertSnapshotNames(loaded);
  }

  @TestTemplate
  void testDropAndRecreateAllocatesNewIdentities() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity original = model(namespace, "sales", "revenue");
    backend.insert(original, false);
    long previousId = liveIds(original.id()).get("revenue");
    assertTrue(backend.delete(original.nameIdentifier(), original.type(), false));
    assertTrue(live(original.id()).isEmpty());
    SemanticModelEntity recreated = model(namespace, original.name(), "revenue");
    backend.insert(recreated, false);
    assertNotEquals(previousId, liveIds(recreated.id()).get("revenue"));
    assertEquals(1, count("semantic_model_metric_meta", original.id()));
  }

  @TestTemplate
  void testDuplicateNamesRollBackCreateAndReplacement() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity duplicate = model(namespace, "duplicate", "same", "same");
    assertThrows(IllegalArgumentException.class, () -> backend.insert(duplicate, false));
    assertEquals(0, count("semantic_model_meta", duplicate.id()));
    assertEquals(0, count("semantic_model_metric_meta", duplicate.id()));
    SemanticModelEntity original = model(namespace, "sales", "original");
    backend.insert(original, false);
    Map<String, Long> before = liveIds(original.id());
    SemanticModelEntity invalid =
        copy(original, original.id(), original.name(), metrics("same", "same"));
    assertThrows(
        IllegalArgumentException.class,
        () -> backend.update(original.nameIdentifier(), original.type(), old -> invalid));
    assertEquals(original, backend.get(original.nameIdentifier(), original.type()));
    assertEquals(before, liveIds(original.id()));
    assertEquals(1, count("semantic_model_version_info", original.id()));
  }

  @TestTemplate
  void testSnapshotFailureRollsBackCreateReplaceAndOverwrite() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity original = model(namespace, "sales", "kept", "removed");
    backend.insert(original, false);
    Map<String, Long> before = liveIds(original.id());
    SemanticModelEntity replacement =
        copy(original, original.id(), original.name(), metrics("kept", "added"));
    SemanticModelEntity created = model(namespace, "new_model", "new_metric");
    try (MockedStatic<SessionUtils> sessions = mockStatic(SessionUtils.class, CALLS_REAL_METHODS)) {
      sessions
          .when(() -> SessionUtils.doWithoutCommit(eq(SemanticModelVersionInfoMapper.class), any()))
          .thenThrow(new IllegalStateException("Injected snapshot failure"));
      assertThrows(IllegalStateException.class, () -> backend.insert(created, false));
      assertThrows(
          IllegalStateException.class,
          () -> backend.update(original.nameIdentifier(), original.type(), old -> replacement));
      assertThrows(IllegalStateException.class, () -> backend.insert(replacement, true));
    }
    assertEquals(original, backend.get(original.nameIdentifier(), original.type()));
    assertEquals(before, liveIds(original.id()));
    assertEquals(2, count("semantic_model_metric_meta", original.id()));
    assertEquals(1, count("semantic_model_version_info", original.id()));
    assertEquals(0, count("semantic_model_meta", created.id()));
    assertEquals(0, count("semantic_model_metric_meta", created.id()));
    assertEquals(0, count("semantic_model_version_info", created.id()));
  }

  @TestTemplate
  void testIdentityInsertFailureRollsBackPartialReconciliation() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity original = model(namespace, "sales", "existing");
    backend.insert(original, false);
    Map<String, Long> before = liveIds(original.id());
    SemanticModelEntity replacement =
        copy(original, original.id(), original.name(), metrics("first", "second"));
    IdGenerator originalGenerator = GravitinoEnv.getInstance().idGenerator();
    AtomicInteger allocated = new AtomicInteger();
    IdGenerator duplicateGenerator =
        () ->
            allocated.incrementAndGet() == 1 ? originalGenerator.nextId() : before.get("existing");
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", duplicateGenerator, true);
    try {
      assertThrows(
          EntityAlreadyExistsException.class,
          () -> backend.update(original.nameIdentifier(), original.type(), old -> replacement));
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", originalGenerator, true);
    }
    assertEquals(2, allocated.get());
    assertEquals(before, liveIds(original.id()));
    assertEquals(1, count("semantic_model_metric_meta", original.id()));
    assertEquals(original, backend.get(original.nameIdentifier(), original.type()));
    assertEquals(1, count("semantic_model_version_info", original.id()));
  }

  @TestTemplate
  void testStaleUpdateAndDropLeaveWinningIdentities() throws Exception {
    SemanticModelEntity original = model(parents(), "sales", "original", "kept");
    backend.insert(original, false);
    SemanticModelPO stale = service().getSemanticModelPOByIdentifier(original.nameIdentifier());
    long keptId = liveIds(original.id()).get("kept");
    SemanticModelEntity winner =
        copy(original, original.id(), original.name(), metrics("winner", "kept"));
    SemanticModelEntity loser = copy(original, original.id(), original.name(), metrics("loser"));
    assertThrows(
        OptimisticLockException.class,
        () ->
            backend.update(
                original.nameIdentifier(),
                original.type(),
                old -> {
                  try {
                    backend.update(original.nameIdentifier(), original.type(), ignored -> winner);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return loser;
                }));
    assertThrows(
        OptimisticLockException.class,
        () -> service().deleteSemanticModelWithVersion(original.nameIdentifier(), stale));
    assertEquals(Set.of("winner", "kept"), liveIds(original.id()).keySet());
    assertEquals(keptId, liveIds(original.id()).get("kept"));
    assertEquals(3, count("semantic_model_metric_meta", original.id()));
    assertEquals(winner, backend.get(original.nameIdentifier(), original.type()));
  }

  @TestTemplate
  void testConcurrentOverwritesKeepIdentityAndSnapshotConsistent() throws Exception {
    SemanticModelEntity original = model(parents(), "sales", "kept", "original");
    backend.insert(original, false);
    long keptId = liveIds(original.id()).get("kept");
    SemanticModelEntity first =
        copy(original, original.id(), original.name(), metrics("kept", "first"));
    SemanticModelEntity second =
        copy(original, original.id(), original.name(), metrics("kept", "second"));
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      List<Future<?>> writes =
          List.of(first, second).stream()
              .map(
                  model ->
                      executor.submit(
                          () -> {
                            ready.countDown();
                            assertTrue(start.await(30, TimeUnit.SECONDS));
                            backend.insert(model, true);
                            return null;
                          }))
              .collect(Collectors.toList());
      assertTrue(ready.await(30, TimeUnit.SECONDS));
      start.countDown();
      for (Future<?> write : writes) {
        write.get(30, TimeUnit.SECONDS);
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
    SemanticModelEntity current = backend.get(original.nameIdentifier(), original.type());
    assertTrue(current.equals(first) || current.equals(second));
    assertSnapshotNames(current);
    assertEquals(keptId, liveIds(original.id()).get("kept"));
    assertEquals(4, count("semantic_model_metric_meta", original.id()));
  }

  @TestTemplate
  void testParentCascadesRetireOnlyTheirMetricsAndAllowCleanup() throws Exception {
    SemanticModelEntity unrelated = model(parents(), "unrelated", "live");
    backend.insert(unrelated, false);
    Map<String, Long> liveBefore = liveIds(unrelated.id());
    for (Entity.EntityType type :
        List.of(Entity.EntityType.SCHEMA, Entity.EntityType.CATALOG, Entity.EntityType.METALAKE)) {
      Namespace namespace = parents();
      SemanticModelEntity original = model(namespace, "sales", "revenue");
      backend.insert(original, false);
      NameIdentifier parent = parent(namespace, type);
      assertTrue(backend.delete(parent, type, true));
      assertTrue(live(original.id()).isEmpty());
      assertEquals(1, count("semantic_model_metric_meta", original.id()));
      // Purge the parent first: metric cleanup must not rely on joining a surviving parent row.
      backend.hardDeleteLegacyData(type, Long.MAX_VALUE);
      backend.hardDeleteLegacyData(Entity.EntityType.SEMANTIC_MODEL, Long.MAX_VALUE);
      assertEquals(0, count("semantic_model_metric_meta", original.id()));
      assertEquals(liveBefore, liveIds(unrelated.id()));
    }
  }

  @TestTemplate
  void testCascadeFailureRollsBackModelAndMetricDeletion() throws Exception {
    SemanticModelEntity original = model(parents(), "sales", "revenue");
    backend.insert(original, false);
    Map<String, Long> before = liveIds(original.id());
    NameIdentifier schema = parent(original.namespace(), Entity.EntityType.SCHEMA);
    assertThrows(
        NonEmptyEntityException.class,
        () -> backend.delete(schema, Entity.EntityType.SCHEMA, false));
    try (MockedStatic<SessionUtils> sessions = mockStatic(SessionUtils.class, CALLS_REAL_METHODS)) {
      sessions
          .when(() -> SessionUtils.doWithoutCommit(eq(SemanticModelMetricMapper.class), any()))
          .thenThrow(new IllegalStateException("Injected identity retirement failure"));
      assertThrows(
          IllegalStateException.class,
          () -> backend.delete(schema, Entity.EntityType.SCHEMA, true));
      assertThrows(
          IllegalStateException.class,
          () -> backend.delete(original.nameIdentifier(), original.type(), false));
    }
    assertEquals(original, backend.get(original.nameIdentifier(), original.type()));
    assertEquals(before, liveIds(original.id()));
  }

  @TestTemplate
  void testGarbageCollectionHonorsTimelineLimitAndLiveIdentities() throws Exception {
    SemanticModelEntity original = model(parents(), "sales", "kept", "removed1", "removed2");
    backend.insert(original, false);
    long keptId = liveIds(original.id()).get("kept");
    SemanticModelEntity updated = copy(original, original.id(), original.name(), metrics("kept"));
    backend.update(original.nameIdentifier(), original.type(), old -> updated);
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "UPDATE semantic_model_metric_meta SET deleted_at = 10 WHERE deleted_at > 0")) {
      assertEquals(2, statement.executeUpdate());
    }
    assertEquals(0, service().deleteSemanticModelMetasByLegacyTimeline(10L, 1));
    assertEquals(1, service().deleteSemanticModelMetasByLegacyTimeline(11L, 1));
    assertEquals(2, count("semantic_model_metric_meta", original.id()));
    assertEquals(1, service().deleteSemanticModelMetasByLegacyTimeline(11L, 1));
    assertEquals(1, backend.deleteOldVersionData(Entity.EntityType.SEMANTIC_MODEL, 1));
    backend.hardDeleteLegacyData(Entity.EntityType.SEMANTIC_MODEL, Long.MAX_VALUE);
    assertEquals(Map.of("kept", keptId), liveIds(original.id()));
    assertEquals(1, count("semantic_model_metric_meta", original.id()));
    assertEquals(1, count("semantic_model_version_info", original.id()));
  }

  private Namespace parents() throws IOException {
    String suffix = Long.toUnsignedString(RandomIdGenerator.INSTANCE.nextId());
    String metalake = "metric_metalake_" + suffix;
    String catalog = "metric_catalog_" + suffix;
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, catalog);
    createAndInsertSchema(metalake, catalog, "schema");
    return Namespace.of(metalake, catalog, "schema");
  }

  private static NameIdentifier parent(Namespace namespace, Entity.EntityType type) {
    switch (type) {
      case SCHEMA:
        return NameIdentifier.of(namespace.levels());
      case CATALOG:
        return NameIdentifier.of(namespace.level(0), namespace.level(1));
      case METALAKE:
        return NameIdentifier.of(namespace.level(0));
      default:
        throw new IllegalArgumentException("Unsupported parent type: " + type);
    }
  }

  private static SemanticModelEntity model(
      Namespace namespace, String name, @Nullable String... names) {
    return SemanticModelEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(AUDIT_INFO)
        .withProperties(Map.of())
        .withDefinition(
            SemanticModelDefinition.builder()
                .withDatasets(
                    new Dataset[] {
                      Dataset.builder()
                          .withName("orders")
                          .withSource(NameIdentifier.of("sales", "orders"))
                          .build()
                    })
                .withMetrics(names == null ? null : metrics(names))
                .build())
        .build();
  }

  private static SemanticModelEntity copy(
      SemanticModelEntity source, Long id, String name, @Nullable Metric[] metrics) {
    return SemanticModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(source.namespace())
        .withComment("updated")
        .withAuditInfo(AUDIT_INFO)
        .withProperties(Map.of("revision", "updated"))
        .withDefinition(
            SemanticModelDefinition.builder()
                .withDatasets(source.definition().datasets())
                .withMetrics(metrics)
                .build())
        .build();
  }

  private static Expression expression(String sql) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder().withDialect("ansi").withExpression(sql).build()
            })
        .build();
  }

  private static Metric[] metrics(String... names) {
    return Arrays.stream(names)
        .map(
            name ->
                Metric.builder()
                    .withName(name)
                    .withExpression(expression("SUM(orders.amount)"))
                    .build())
        .toArray(Metric[]::new);
  }

  private static SemanticModelMetaService service() {
    return SemanticModelMetaService.getInstance();
  }

  private static List<SemanticModelMetricPO> live(Long modelId) {
    return SessionUtils.getWithoutCommit(
        SemanticModelMetricMapper.class,
        mapper -> mapper.selectSemanticModelMetricsByModelIdForUpdate(modelId));
  }

  private static Map<String, Long> liveIds(Long modelId) {
    return live(modelId).stream()
        .collect(
            Collectors.toMap(SemanticModelMetricPO::metricName, SemanticModelMetricPO::metricId));
  }

  private static void assertSnapshotNames(SemanticModelEntity model) {
    Set<String> names =
        Arrays.stream(model.definition().metrics()).map(Metric::name).collect(Collectors.toSet());
    assertEquals(names, liveIds(model.id()).keySet());
  }

  private static int count(String table, long modelId) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT COUNT(*) FROM " + table + " WHERE semantic_model_id = ?")) {
      statement.setLong(1, modelId);
      try (ResultSet results = statement.executeQuery()) {
        assertTrue(results.next());
        return results.getInt(1);
      }
    }
  }
}
