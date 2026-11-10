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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.semantic.SemanticModelTestUtils.sources;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelSource;
import org.apache.gravitino.semantic.SemanticModelSourceUpdate;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Verifies managed create and load through a real relational persistence backend. */
public class TestManagedSemanticModelOperationsJDBC extends TestJDBCBackend {

  private final AtomicInteger writeValidationCount = new AtomicInteger();

  private Config previousConfig;
  private NameIdentifier modelIdent;
  private ManagedSemanticModelOperations operations;

  @BeforeAll
  public void captureEnvironmentConfig() {
    // BackendTestExtension replaces GravitinoEnv.config before each backend invocation.
    previousConfig = GravitinoEnv.getInstance().config();
  }

  @BeforeEach
  public void prepareManagedOperations() throws IOException, IllegalAccessException {
    writeValidationCount.set(0);
    String metalake = "managed_semantic_model_metalake";
    String catalog = "managed_semantic_model_catalog";
    String schema = "managed_semantic_model_schema";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, catalog);
    createAndInsertSchema(metalake, catalog, schema);

    Namespace namespace = NamespaceUtil.ofSemanticModel(metalake, catalog, schema);
    modelIdent = NameIdentifier.of(namespace, "sales_model");

    Config config = new Config(false) {};
    config.set(Configs.CACHE_ENABLED, false);
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);
    operations =
        new ManagedSemanticModelOperations(
            store,
            RandomIdGenerator.INSTANCE,
            (ident, definition) -> {
              SemanticModelValidator.validateDefinition(definition);
              writeValidationCount.incrementAndGet();
            });
  }

  @AfterEach
  public void restoreEnvironmentConfig() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
  }

  @TestTemplate
  public void testCreateThenLoadRoundTrip() throws Exception {
    SemanticModelDefinition definition = definition("orders");

    SemanticModel created =
        operations.createSemanticModel(
            modelIdent,
            "Persisted model",
            definition,
            Map.of("domain", "sales"),
            sources(definition));
    SemanticModel loaded = operations.loadSemanticModel(modelIdent);

    assertNotSame(created, loaded);
    assertEquals(created, loaded);
    assertEquals(definition, loaded.definition());
    assertEquals(1, writeValidationCount.get());
    assertTrue(backend.exists(modelIdent, Entity.EntityType.SEMANTIC_MODEL));
    assertEquals(
        sources(definition).sources(),
        liveAssociations(((SemanticModelEntity) created).id()).stream()
            .map(Association::source)
            .toList());
    assertFalse(((SemanticModelEntity) loaded).sourceUpdate().replacesSources());
  }

  @TestTemplate
  public void testListAlterAndDropRoundTrip() throws Exception {
    SemanticModel created =
        operations.createSemanticModel(
            modelIdent,
            "Original",
            definition("orders"),
            Map.of("domain", "sales"),
            sources(definition("orders")));
    NameIdentifier renamedIdent = NameIdentifier.of(modelIdent.namespace(), "renamed_sales_model");
    SemanticModelDefinition replacement = definition("invoices");

    SemanticModel altered =
        operations.alterSemanticModel(
            modelIdent,
            sources(replacement),
            SemanticModelChange.rename(renamedIdent.name()),
            SemanticModelChange.updateComment("Updated"),
            SemanticModelChange.removeProperty("domain"),
            SemanticModelChange.setProperty("tier", "gold"),
            SemanticModelChange.replaceDefinition(replacement));

    assertEquals(renamedIdent.name(), altered.name());
    assertEquals("Updated", altered.comment());
    assertEquals(replacement, altered.definition());
    assertEquals(Map.of("tier", "gold"), altered.properties());
    assertEquals(created.auditInfo().creator(), altered.auditInfo().creator());
    assertEquals(created.auditInfo().createTime(), altered.auditInfo().createTime());
    assertEquals(created.auditInfo().creator(), altered.auditInfo().lastModifier());
    assertNotNull(altered.auditInfo().lastModifiedTime());
    assertEquals(2, writeValidationCount.get());
    assertArrayEquals(
        new NameIdentifier[] {renamedIdent}, operations.listSemanticModels(modelIdent.namespace()));
    assertFalse(backend.exists(modelIdent, Entity.EntityType.SEMANTIC_MODEL));
    assertTrue(backend.exists(renamedIdent, Entity.EntityType.SEMANTIC_MODEL));
    assertEquals(altered, operations.loadSemanticModel(renamedIdent));
    long modelId = ((SemanticModelEntity) created).id();
    assertEquals(
        sources(replacement).sources(),
        liveAssociations(modelId).stream().map(Association::source).toList());
    assertTrue(operations.dropSemanticModel(renamedIdent));
    assertTrue(liveAssociations(modelId).isEmpty());
    assertFalse(operations.dropSemanticModel(renamedIdent));
  }

  @TestTemplate
  public void testRejectedAlterIsAtomicAndRenameConflictIsTyped() throws Exception {
    SemanticModel original =
        operations.createSemanticModel(
            modelIdent,
            "Original",
            definition("orders"),
            Map.of("owner", "sales"),
            sources(definition("orders")));
    List<Association> originalAssociations =
        liveAssociations(((SemanticModelEntity) original).id());
    NameIdentifier existingIdent = NameIdentifier.of(modelIdent.namespace(), "existing_model");
    operations.createSemanticModel(
        existingIdent, null, definition("customers"), Map.of(), sources(definition("customers")));
    SemanticModelDefinition duplicateDatasets =
        SemanticModelDefinition.builder()
            .withDatasets(
                new Dataset[] {
                  definition("duplicate").datasets()[0], definition("duplicate").datasets()[0]
                })
            .build();

    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            operations.alterSemanticModel(
                modelIdent,
                sources(duplicateDatasets),
                SemanticModelChange.updateComment("Must not persist"),
                SemanticModelChange.setProperty("owner", "changed"),
                SemanticModelChange.replaceDefinition(duplicateDatasets)));
    assertThrows(
        SemanticModelAlreadyExistsException.class,
        () ->
            operations.alterSemanticModel(
                modelIdent,
                SemanticModelSourceUpdate.preserve(),
                SemanticModelChange.rename(existingIdent.name())));

    SemanticModel loaded = operations.loadSemanticModel(modelIdent);
    assertEquals(original, loaded);
    assertEquals(originalAssociations, liveAssociations(((SemanticModelEntity) loaded).id()));
    assertEquals("existing_model", operations.loadSemanticModel(existingIdent).name());
    assertEquals(2, writeValidationCount.get());
  }

  @TestTemplate
  public void testExplicitRebindingAndMetadataPreservationThroughEntityStore() throws Exception {
    TableEntity source = createAndInsertTableEntity(modelIdent.namespace(), "orders");
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(
                new Dataset[] {
                  Dataset.builder()
                      .withName("orders")
                      .withSource(
                          NameIdentifier.of(
                              modelIdent.namespace().level(1),
                              modelIdent.namespace().level(2),
                              source.name()))
                      .build()
                })
            .build();
    SemanticModelSourceUpdate initial =
        SemanticModelSourceUpdate.replace(
            List.of(new SemanticModelSource("orders", Entity.EntityType.TABLE, source.id())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            operations.createSemanticModel(
                modelIdent, null, definition, Map.of(), SemanticModelSourceUpdate.preserve()));
    SemanticModelEntity created =
        (SemanticModelEntity)
            operations.createSemanticModel(modelIdent, null, definition, Map.of(), initial);
    List<Association> original = liveAssociations(created.id());
    assertEquals(initial.sources(), original.stream().map(Association::source).toList());

    backend.delete(source.nameIdentifier(), source.type(), false);
    NameIdentifier renamedIdent = NameIdentifier.of(modelIdent.namespace(), "renamed_model");
    operations.alterSemanticModel(
        modelIdent,
        SemanticModelSourceUpdate.preserve(),
        SemanticModelChange.rename(renamedIdent.name()),
        SemanticModelChange.updateComment("updated after deletion"),
        SemanticModelChange.setProperty("owner", "analytics"));
    assertEquals(original, liveAssociations(created.id()));
    assertEquals(1, writeValidationCount.get());

    TableEntity recreated = createAndInsertTableEntity(modelIdent.namespace(), "orders");
    SemanticModelSourceUpdate rebound =
        SemanticModelSourceUpdate.replace(
            List.of(new SemanticModelSource("orders", Entity.EntityType.TABLE, recreated.id())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            operations.alterSemanticModel(
                renamedIdent,
                SemanticModelSourceUpdate.preserve(),
                SemanticModelChange.replaceDefinition(definition)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            operations.alterSemanticModel(
                renamedIdent, rebound, SemanticModelChange.updateComment("invalid intent")));
    assertEquals(original, liveAssociations(created.id()));

    // Explicit replacement rebinds the source even though the definition is unchanged.
    operations.alterSemanticModel(
        renamedIdent, rebound, SemanticModelChange.replaceDefinition(definition));
    assertEquals(
        rebound.sources(),
        liveAssociations(created.id()).stream().map(Association::source).toList());
    assertEquals(definition, operations.loadSemanticModel(renamedIdent).definition());
    assertEquals(2, writeValidationCount.get());
  }

  /**
   * Verifies that a replacement retains the resolved source ID if the source is recreated before
   * the model write.
   *
   * @throws Exception if persistence fails
   */
  @TestTemplate
  public void testReplacementRetainsResolvedIdAfterSourceRecreation() throws Exception {
    TableEntity source = createAndInsertTableEntity(modelIdent.namespace(), "orders");
    NameIdentifier sourceIdent =
        NameIdentifier.of(
            modelIdent.namespace().level(1), modelIdent.namespace().level(2), source.name());
    SemanticModelDefinition initialDefinition = definition("orders", sourceIdent);
    SemanticModelEntity created =
        (SemanticModelEntity)
            operations.createSemanticModel(
                modelIdent,
                null,
                initialDefinition,
                Map.of(),
                SemanticModelSourceUpdate.replace(
                    List.of(
                        new SemanticModelSource("orders", Entity.EntityType.TABLE, source.id()))));

    SemanticModelDefinition replacementDefinition = definition("recent_orders", sourceIdent);
    SemanticModelSourceUpdate resolvedUpdate =
        SemanticModelSourceUpdate.replace(
            List.of(
                new SemanticModelSource("recent_orders", Entity.EntityType.TABLE, source.id())));

    // Reproduce source recreation after validation has resolved the ID, but before the model write.
    assertTrue(backend.delete(source.nameIdentifier(), source.type(), false));
    TableEntity recreated = createAndInsertTableEntity(modelIdent.namespace(), source.name());
    assertNotEquals(source.id(), recreated.id());

    operations.alterSemanticModel(
        modelIdent, resolvedUpdate, SemanticModelChange.replaceDefinition(replacementDefinition));

    assertEquals(replacementDefinition, operations.loadSemanticModel(modelIdent).definition());
    assertEquals(
        resolvedUpdate.sources(),
        liveAssociations(created.id()).stream().map(Association::source).toList());
    assertEquals(2, writeValidationCount.get());
  }

  private List<Association> liveAssociations(long modelId) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT id, dataset_name, source_type, source_id FROM semantic_model_source_rel"
                        + " WHERE semantic_model_id = ? AND deleted_at = 0 ORDER BY id")) {
      statement.setLong(1, modelId);
      try (ResultSet result = statement.executeQuery()) {
        List<Association> rows = new ArrayList<>();
        while (result.next()) {
          rows.add(
              new Association(
                  result.getLong(1),
                  new SemanticModelSource(
                      result.getString(2),
                      Entity.EntityType.valueOf(result.getString(3)),
                      result.getLong(4))));
        }
        return rows;
      }
    }
  }

  @SuppressWarnings("UnusedVariable") // Error Prone does not recognize record component accessors.
  private record Association(long id, SemanticModelSource source) {}

  private static SemanticModelDefinition definition(String datasetName) {
    return definition(
        datasetName, NameIdentifier.of("source_catalog", "source_schema", datasetName));
  }

  private static SemanticModelDefinition definition(String datasetName, NameIdentifier source) {
    Dataset dataset =
        Dataset.builder()
            .withName(datasetName)
            .withSource(source)
            .withPrimaryKey(new String[0])
            .withUniqueKeys(new String[0][])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    return SemanticModelDefinition.builder()
        .withDatasets(new Dataset[] {dataset})
        .withRelationships(new Relationship[0])
        .withMetrics((Metric[]) null)
        .withCustomExtensions(new CustomExtension[0])
        .build();
  }
}
