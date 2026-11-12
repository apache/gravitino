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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.ManagedSemanticModelOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.MockedStatic;

/** Verifies metric projections and committed read consistency on every supported database. */
public class TestSemanticModelMetricDiscovery extends TestJDBCBackend {

  @TestTemplate
  void testEmptyMissingAndSchemaIsolation() throws Exception {
    Namespace namespace = parents();
    ManagedSemanticModelOperations operations = operations();
    assertEquals(0, operations.listMetrics(namespace).length);
    assertEquals(0, operations.listMetricInfos(namespace).length);
    backend.insert(model(namespace, "absent", (Metric[]) null), false);
    backend.insert(model(namespace, "empty"), false);
    backend.insert(model(parents(), "elsewhere", metric("revenue")), false);
    assertEquals(0, operations.listMetrics(namespace).length);
    assertEquals(0, operations.listMetricInfos(namespace).length);
    Namespace missing = Namespace.of(namespace.level(0), namespace.level(1), "missing");
    assertThrows(NoSuchSchemaException.class, () -> operations.listMetrics(missing));
    assertThrows(NoSuchSchemaException.class, () -> operations.listMetricInfos(missing));
    assertThrows(
        IllegalArgumentException.class,
        () -> backend.listSemanticModelMetrics(Namespace.of("invalid")));
  }

  @TestTemplate
  void testCompleteMetricsStructuredOrderingAndNames() throws Exception {
    Namespace namespace = parents();
    Metric[] metrics = {
      metric("z"), metric("revenue"), metric("Revenue"), metric("收入.'"), metric("x".repeat(300))
    };
    backend.insert(model(namespace, "a-b", metric("a")), false);
    backend.insert(model(namespace, "a", metrics), false);
    ManagedSemanticModelOperations operations = operations();
    NameIdentifier[] names = operations.listMetrics(namespace);
    List<String> sorted = Arrays.stream(metrics).map(Metric::name).sorted().toList();
    for (int i = 0; i < sorted.size(); i++) {
      assertEquals(identifier(namespace, "a", sorted.get(i)), names[i]);
    }
    assertEquals(identifier(namespace, "a-b", "a"), names[5]);
    SemanticModelMetric[] infos = operations.listMetricInfos(namespace);
    assertArrayEquals(names, identifiers(infos));
    for (SemanticModelMetric info : infos) {
      assertEquals(metric(info.metric().name()), info.metric());
    }
  }

  @TestTemplate
  void testHierarchicalSchemaPreservesLogicalNamespace() throws Exception {
    Namespace parent = parents();
    String schema = "outer" + HierarchicalSchemaUtil.schemaSeparator() + "inner";
    createAndInsertSchema(parent.level(0), parent.level(1), schema);
    Namespace namespace = Namespace.of(parent.level(0), parent.level(1), schema);
    backend.insert(model(namespace, "sales", metric("Revenue")), false);
    NameIdentifier expected = identifier(namespace, "sales", "Revenue");
    assertEquals(List.of(expected), backend.listSemanticModelMetrics(namespace));
    assertEquals(expected, identifiers(operations().listMetricInfos(namespace))[0]);
  }

  @TestTemplate
  void testRenameReplacementDropAndRecreationReadOnlyCurrentState() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity original = model(namespace, "before", metric("removed"), metric("kept"));
    backend.insert(original, false);
    SemanticModelEntity replacement = copy(original, "after", metric("added"), metric("kept"));
    backend.update(original.nameIdentifier(), original.type(), old -> replacement);
    ManagedSemanticModelOperations operations = operations();
    NameIdentifier[] expected = {
      identifier(namespace, "after", "added"), identifier(namespace, "after", "kept")
    };
    assertArrayEquals(expected, operations.listMetrics(namespace));
    assertArrayEquals(expected, identifiers(operations.listMetricInfos(namespace)));
    backend.delete(replacement.nameIdentifier(), replacement.type(), false);
    assertEquals(0, operations.listMetrics(namespace).length);
    assertEquals(0, operations.listMetricInfos(namespace).length);
    backend.insert(model(namespace, "after", metric("new")), false);
    backend.hardDeleteLegacyData(Entity.EntityType.SEMANTIC_MODEL, Long.MAX_VALUE);
    assertArrayEquals(
        new NameIdentifier[] {identifier(namespace, "after", "new")},
        operations.listMetrics(namespace));
    assertArrayEquals(
        operations.listMetrics(namespace), identifiers(operations.listMetricInfos(namespace)));
  }

  @TestTemplate
  void testDiscoveryDoesNotDecodeUnrelatedFields() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity model = model(namespace, "sales", metric("revenue"));
    backend.insert(model, false);
    execute(
        "UPDATE semantic_model_version_info SET datasets = ?, relationships = ?, ai_context = ?, custom_extensions = ? WHERE semantic_model_id = ?",
        "invalid",
        "invalid",
        "invalid",
        "invalid",
        model.id());
    assertThrows(RuntimeException.class, () -> backend.get(model.nameIdentifier(), model.type()));
    assertEquals(
        List.of(identifier(namespace, "sales", "revenue")),
        backend.listSemanticModelMetrics(namespace));
    assertEquals(
        metric("revenue"), backend.listSemanticModelMetricInfos(namespace).get(0).metric());
  }

  @TestTemplate
  void testNamesDoNotReadSnapshotsAndDetailsDoNotReadIdentities() throws Exception {
    Namespace namespace = parents();
    SemanticModelEntity model = model(namespace, "sales", metric("revenue"));
    backend.insert(model, false);
    execute("DELETE FROM semantic_model_metric_meta WHERE semantic_model_id = ?", model.id());
    assertTrue(backend.listSemanticModelMetrics(namespace).isEmpty());
    assertEquals(
        metric("revenue"), backend.listSemanticModelMetricInfos(namespace).get(0).metric());
    SemanticModelEntity independent = model(namespace, "names_only", metric("count"));
    backend.insert(independent, false);
    execute(
        "DELETE FROM semantic_model_version_info WHERE semantic_model_id = ?", independent.id());
    assertEquals(
        List.of(identifier(namespace, "names_only", "count")),
        backend.listSemanticModelMetrics(namespace));
    assertThrows(
        IllegalStateException.class, () -> backend.listSemanticModelMetricInfos(namespace));
  }

  @TestTemplate
  void testMalformedMetricsFailWholeDetailedRead() throws Exception {
    Namespace namespace = parents();
    backend.insert(model(namespace, "valid", metric("good")), false);
    SemanticModelEntity broken = model(namespace, "broken", metric("bad"));
    backend.insert(broken, false);
    for (String json : List.of("not-json", "null", "{}", "[null]", "[{\"name\":\"bad\"}]")) {
      execute(
          "UPDATE semantic_model_version_info SET metrics = ? WHERE semantic_model_id = ?",
          json,
          broken.id());
      assertEquals(2, backend.listSemanticModelMetrics(namespace).size());
      assertThrows(
          IllegalStateException.class, () -> backend.listSemanticModelMetricInfos(namespace), json);
    }
  }

  @TestTemplate
  void testParentDeletionDoesNotExposeRetiredMetrics() throws Exception {
    Namespace unrelated = parents();
    backend.insert(model(unrelated, "other", metric("live")), false);
    for (Entity.EntityType type :
        List.of(Entity.EntityType.SCHEMA, Entity.EntityType.CATALOG, Entity.EntityType.METALAKE)) {
      Namespace namespace = parents();
      backend.insert(model(namespace, "sales", metric("revenue")), false);
      NameIdentifier parent =
          type == Entity.EntityType.SCHEMA
              ? NameIdentifier.of(namespace.levels())
              : type == Entity.EntityType.CATALOG
                  ? NameIdentifier.of(namespace.level(0), namespace.level(1))
                  : NameIdentifier.of(namespace.level(0));
      backend.delete(parent, type, true);
      assertThrows(NoSuchEntityException.class, () -> backend.listSemanticModelMetrics(namespace));
      assertThrows(
          NoSuchEntityException.class, () -> backend.listSemanticModelMetricInfos(namespace));
      assertEquals(1, backend.listSemanticModelMetrics(unrelated).size());
      assertEquals(1, backend.listSemanticModelMetricInfos(unrelated).size());
    }
  }

  @TestTemplate
  void testReadsDuringReplaceAndRenameSeeOnlyCommittedState() throws Exception {
    for (boolean rollback : List.of(false, true)) {
      Namespace namespace = parents();
      SemanticModelEntity original = model(namespace, "before", metric("before"));
      backend.insert(original, false);
      SemanticModelEntity replacement = copy(original, "after", metric("after"));
      CountDownLatch staged = new CountDownLatch(1);
      CountDownLatch finish = new CountDownLatch(1);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> writer =
            executor.submit(
                () -> {
                  try (MockedStatic<SessionUtils> sessions =
                      mockStatic(SessionUtils.class, CALLS_REAL_METHODS)) {
                    sessions
                        .when(
                            () ->
                                SessionUtils.doWithoutCommit(
                                    eq(SemanticModelVersionInfoMapper.class), any()))
                        .thenAnswer(
                            invocation -> {
                              staged.countDown();
                              assertTrue(finish.await(30, TimeUnit.SECONDS));
                              if (rollback) throw new IllegalStateException("Injected rollback");
                              return invocation.callRealMethod();
                            });
                    if (rollback) {
                      assertThrows(
                          IllegalStateException.class,
                          () ->
                              backend.update(
                                  original.nameIdentifier(), original.type(), old -> replacement));
                    } else {
                      backend.update(
                          original.nameIdentifier(), original.type(), old -> replacement);
                    }
                  }
                  return null;
                });
        assertTrue(staged.await(30, TimeUnit.SECONDS));
        // The writer has updated the name, version pointer, and identities, but has not committed.
        assertArrayEquals(
            new NameIdentifier[] {identifier(namespace, "before", "before")},
            operations().listMetrics(namespace));
        assertArrayEquals(
            new NameIdentifier[] {identifier(namespace, "before", "before")},
            identifiers(operations().listMetricInfos(namespace)));
        finish.countDown();
        writer.get(30, TimeUnit.SECONDS);
        String committed = rollback ? "before" : "after";
        assertArrayEquals(
            new NameIdentifier[] {identifier(namespace, committed, committed)},
            operations().listMetrics(namespace));
        assertArrayEquals(
            new NameIdentifier[] {identifier(namespace, committed, committed)},
            identifiers(operations().listMetricInfos(namespace)));
      } finally {
        finish.countDown();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
      }
    }
  }

  private ManagedSemanticModelOperations operations() throws IllegalAccessException {
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    return new ManagedSemanticModelOperations(
        store, RandomIdGenerator.INSTANCE, (ident, definition) -> {});
  }

  private Namespace parents() throws IOException {
    String suffix = Long.toUnsignedString(RandomIdGenerator.INSTANCE.nextId());
    String metalake = "discovery_metalake_" + suffix;
    String catalog = "discovery_catalog_" + suffix;
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, catalog);
    createAndInsertSchema(metalake, catalog, "schema");
    return Namespace.of(metalake, catalog, "schema");
  }

  private static SemanticModelEntity model(
      Namespace namespace, String name, @Nullable Metric... metrics) {
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
                .withMetrics(metrics)
                .build())
        .build();
  }

  private static SemanticModelEntity copy(
      SemanticModelEntity original, String name, Metric... metrics) {
    return SemanticModelEntity.builder()
        .withId(original.id())
        .withName(name)
        .withNamespace(original.namespace())
        .withAuditInfo(AUDIT_INFO)
        .withProperties(Map.of())
        .withDefinition(
            SemanticModelDefinition.builder()
                .withDatasets(original.definition().datasets())
                .withMetrics(metrics)
                .build())
        .build();
  }

  private static Metric metric(String name) {
    return Metric.builder()
        .withName(name)
        .withDescription("A complete metric")
        .withExpression(
            Expression.builder()
                .withDialects(
                    new DialectExpression[] {
                      DialectExpression.builder()
                          .withDialect("ansi")
                          .withExpression("SUM(orders.amount)")
                          .build()
                    })
                .build())
        .withDatatype(DataType.FLOAT)
        .withAIContext(AIContext.of("Use this metric"))
        .withCustomExtensions(
            new CustomExtension[] {
              CustomExtension.builder().withVendorName("vendor").withData("{\"key\":1}").build()
            })
        .build();
  }

  private static NameIdentifier identifier(Namespace namespace, String model, String metric) {
    return NameIdentifier.of(
        Namespace.of(namespace.level(0), namespace.level(1), namespace.level(2), model), metric);
  }

  private static NameIdentifier[] identifiers(SemanticModelMetric[] metrics) {
    return Arrays.stream(metrics)
        .map(info -> NameIdentifier.of(info.namespace(), info.metric().name()))
        .toArray(NameIdentifier[]::new);
  }

  private static void execute(String sql, Object... args) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        PreparedStatement statement = session.getConnection().prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) statement.setObject(i + 1, args[i]);
      statement.executeUpdate();
    }
  }
}
