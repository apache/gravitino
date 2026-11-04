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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelSource;
import org.apache.gravitino.semantic.SemanticModelSourceUpdate;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Verifies association writes, retirement, rollback and GC on every relational backend. */
public class TestSemanticModelSourceAssociations extends TestJDBCBackend {
  private Namespace sourceNamespace;
  private Namespace modelNamespace;
  private TableEntity source;
  private long metalakeId;

  @BeforeEach
  void prepareSources() throws Exception {
    metalakeId = createAndInsertMakeLake("lake").id();
    createAndInsertCatalog("lake", "sources");
    createAndInsertCatalog("lake", "models");
    createAndInsertSchema("lake", "sources", "hr");
    createAndInsertSchema("lake", "models", "semantic");
    sourceNamespace = Namespace.of("lake", "sources", "hr");
    modelNamespace = Namespace.of("lake", "models", "semantic");
    source = createAndInsertTableEntity(sourceNamespace, "employees");
  }

  @TestTemplate
  void testSharedSourcesPreserveExactDatasetNames() throws Exception {
    String[] names = {"employee", "Employee", "manager.with.dot", "x".repeat(1200), "员工'"};
    SemanticModelEntity model = model("staff", source.id(), names);
    backend.insert(model, false);
    List<Association> live = live(model.id());
    assertEquals(
        Set.of(names), live.stream().map(Association::datasetName).collect(Collectors.toSet()));
    assertEquals(names.length, live.stream().map(Association::id).distinct().count());
    assertTrue(live.stream().allMatch(row -> row.metalakeId() == metalakeId));
    assertTrue(live.stream().allMatch(row -> row.sourceId() == source.id()));
    assertTrue(live.stream().allMatch(row -> row.sourceType().equals("TABLE")));
    SemanticModelEntity loaded = service().getSemanticModelByIdentifier(model.nameIdentifier());
    assertEquals(model, loaded);
    assertFalse(loaded.sourceUpdate().replacesSources());
  }

  @TestTemplate
  void testTableAndViewAssociationsKeepTheirTypes() throws Exception {
    SemanticModelEntity model = model("staff", source.id(), "table", "view");
    List<SemanticModelSource> sources =
        List.of(
            new SemanticModelSource("table", Entity.EntityType.TABLE, source.id()),
            new SemanticModelSource("view", Entity.EntityType.VIEW, source.id()));
    backend.insert(
        copy(model, model.name(), model.definition(), SemanticModelSourceUpdate.replace(sources)),
        false);
    assertEquals(Set.copyOf(sources), sourceSet(model.id()));
  }

  @TestTemplate
  void testReplaceEntireSetAndRetirePreviousRows() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee", "manager");
    backend.insert(original, false);
    Set<Long> initialIds = liveIds(original.id());
    SemanticModelEntity changed = model("staff", source.id(), "manager", "renamed", "added");
    backend.update(
        original.nameIdentifier(),
        original.type(),
        old -> copy(original, "staff", changed.definition(), changed.sourceUpdate()));
    assertEquals(Set.of("manager", "renamed", "added"), datasetNames(original.id()));
    assertTrue(liveIds(original.id()).stream().noneMatch(initialIds::contains));
    List<Association> retired =
        rows(original.id()).stream().filter(row -> row.deletedAt() > 0).toList();
    assertEquals(2, retired.size());

    SemanticModelEntity reordered = model("staff", source.id(), "added", "renamed", "manager");
    backend.update(
        original.nameIdentifier(),
        original.type(),
        old -> copy(original, "staff", reordered.definition(), reordered.sourceUpdate()));
    assertEquals(Set.of("manager", "renamed", "added"), datasetNames(original.id()));
    assertTrue(rows(original.id()).containsAll(retired));
    assertEquals(8, rows(original.id()).size());
    assertEquals(
        3, service().getSemanticModelPOByIdentifier(original.nameIdentifier()).getCurrentVersion());
  }

  @TestTemplate
  void testMetadataUpdatesPreserveRowsAndExplicitReplacementRebinds() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee");
    backend.insert(original, false);
    List<Association> initial = rows(original.id());
    assertTrue(backend.delete(source.nameIdentifier(), source.type(), false));
    TableEntity replacement = createAndInsertTableEntity(sourceNamespace, source.name());
    assertNotEquals(source.id(), replacement.id());
    SemanticModelEntity renamed =
        copy(original, "renamed", original.definition(), SemanticModelSourceUpdate.preserve());
    backend.update(original.nameIdentifier(), original.type(), old -> renamed);
    assertEquals(initial, rows(original.id()));
    assertEquals(renamed, service().getSemanticModelByIdentifier(renamed.nameIdentifier()));

    SemanticModelSourceUpdate rebound =
        SemanticModelSourceUpdate.replace(
            List.of(new SemanticModelSource("employee", source.type(), replacement.id())));
    backend.update(
        renamed.nameIdentifier(),
        renamed.type(),
        old -> copy(renamed, renamed.name(), renamed.definition(), rebound));
    assertEquals(Set.copyOf(rebound.sources()), sourceSet(original.id()));
    assertEquals(2, rows(original.id()).size());
    assertTrue(rows(original.id()).get(0).deletedAt() > 0);
  }

  @TestTemplate
  void testSourceRenamePreservesAssociationsAndDefinition() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee");
    backend.insert(original, false);
    List<Association> initial = rows(original.id());
    TableEntity renamed =
        createTableEntity(source.id(), sourceNamespace, "employees_v2", AUDIT_INFO);
    backend.update(source.nameIdentifier(), source.type(), old -> renamed);
    assertEquals(initial, rows(original.id()));
    assertEquals(original, service().getSemanticModelByIdentifier(original.nameIdentifier()));
  }

  @TestTemplate
  void testOverwriteUsesPersistedModelIdentity() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee");
    backend.insert(original, false);
    SemanticModelEntity overwrite = model("staff", source.id(), "manager");
    assertNotEquals(original.id(), overwrite.id());
    backend.insert(overwrite, true);
    assertEquals(
        original.id(), service().getSemanticModelByIdentifier(original.nameIdentifier()).id());
    assertEquals(Set.of("manager"), datasetNames(original.id()));
    assertEquals(2, rows(original.id()).size());
    assertTrue(rows(overwrite.id()).isEmpty());
  }

  @TestTemplate
  void testInvalidAssociationSetRollsBackCreateAndUpdate() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee");
    backend.insert(original, false);
    List<Association> initial = rows(original.id());
    SemanticModelSourceUpdate invalid =
        SemanticModelSourceUpdate.replace(
            List.of(new SemanticModelSource("wrong", source.type(), source.id())));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            backend.update(
                original.nameIdentifier(),
                original.type(),
                old -> copy(original, "renamed", original.definition(), invalid)));
    assertEquals(original, service().getSemanticModelByIdentifier(original.nameIdentifier()));
    assertEquals(initial, rows(original.id()));
    assertEquals(
        1, service().getSemanticModelPOByIdentifier(original.nameIdentifier()).getCurrentVersion());
    SemanticModelEntity invalidCreate =
        copy(model("invalid", source.id(), "employee"), "invalid", original.definition(), invalid);
    assertThrows(IllegalArgumentException.class, () -> backend.insert(invalidCreate, false));
    assertThrows(
        NoSuchEntityException.class,
        () -> service().getSemanticModelByIdentifier(invalidCreate.nameIdentifier()));
    assertTrue(rows(invalidCreate.id()).isEmpty());
  }

  @TestTemplate
  void testBatchedInsertAndFailureRollBackRetirementAndSnapshot() throws Exception {
    String[] names = IntStream.range(0, 1001).mapToObj(i -> "dataset_" + i).toArray(String[]::new);
    SemanticModelEntity original = model("staff", source.id(), names);
    backend.insert(original, false);
    List<Association> initial = rows(original.id());
    assertEquals(1001, initial.size());
    TableEntity blocked = createAndInsertTableEntity(sourceNamespace, "blocked");
    List<SemanticModelSource> sources = new ArrayList<>(original.sourceUpdate().sources());
    sources.set(1000, new SemanticModelSource(names[1000], source.type(), blocked.id()));
    addConstraint("reject_source", "source_id <> " + blocked.id());
    try {
      IOException failure =
          assertThrows(
              IOException.class,
              () ->
                  backend.update(
                      original.nameIdentifier(),
                      original.type(),
                      old ->
                          copy(
                              original,
                              "renamed",
                              original.definition(),
                              SemanticModelSourceUpdate.replace(sources))));
      assertTrue(
          ExceptionUtils.getStackTrace(failure).toLowerCase(Locale.ROOT).contains("reject_source"));
      assertEquals(initial, rows(original.id()));
      assertEquals(original, service().getSemanticModelByIdentifier(original.nameIdentifier()));
      assertEquals(
          1,
          service().getSemanticModelPOByIdentifier(original.nameIdentifier()).getCurrentVersion());
      assertEquals(1, snapshotCount(original.id()));
      assertThrows(
          IOException.class,
          () ->
              backend.insert(
                  copy(
                      original,
                      original.name(),
                      original.definition(),
                      SemanticModelSourceUpdate.replace(sources)),
                  true));
      assertEquals(initial, rows(original.id()));
      assertEquals(original, service().getSemanticModelByIdentifier(original.nameIdentifier()));
      assertEquals(1, snapshotCount(original.id()));
      SemanticModelEntity invalidCreate = model("invalid", blocked.id(), "employee");
      assertThrows(IOException.class, () -> backend.insert(invalidCreate, false));
      assertThrows(
          NoSuchEntityException.class,
          () -> service().getSemanticModelByIdentifier(invalidCreate.nameIdentifier()));
      assertTrue(rows(invalidCreate.id()).isEmpty());
      assertEquals(0, snapshotCount(invalidCreate.id()));
    } finally {
      dropConstraint("reject_source");
    }
  }

  @TestTemplate
  void testModelAndParentDeletionRetireOnlyOwnedAssociations() throws Exception {
    for (Entity.EntityType type :
        List.of(
            Entity.EntityType.SEMANTIC_MODEL,
            Entity.EntityType.SCHEMA,
            Entity.EntityType.CATALOG,
            Entity.EntityType.METALAKE)) {
      resetDatabase();
      prepareSources();
      SemanticModelEntity original = model("staff", source.id(), "employee");
      backend.insert(original, false);
      NameIdentifier target = deletionTarget(original, type);
      assertTrue(backend.delete(target, type, true));
      assertTrue(live(original.id()).isEmpty());
      assertEquals(1, rows(original.id()).size());
      assertTrue(rows(original.id()).get(0).deletedAt() > 0);
      assertThrows(
          NoSuchEntityException.class,
          () -> service().getSemanticModelByIdentifier(original.nameIdentifier()));
    }
  }

  @TestTemplate
  void testRetirementFailureRollsBackModelAndEveryParentCascade() throws Exception {
    for (Entity.EntityType type :
        List.of(
            Entity.EntityType.SEMANTIC_MODEL,
            Entity.EntityType.SCHEMA,
            Entity.EntityType.CATALOG,
            Entity.EntityType.METALAKE)) {
      resetDatabase();
      prepareSources();
      SemanticModelEntity original = model("staff", source.id(), "employee");
      backend.insert(original, false);
      List<Association> initial = rows(original.id());
      NameIdentifier target = deletionTarget(original, type);
      addConstraint("reject_retirement", "deleted_at = 0");
      try {
        Exception failure = assertThrows(Exception.class, () -> backend.delete(target, type, true));
        assertTrue(
            ExceptionUtils.getStackTrace(failure)
                .toLowerCase(Locale.ROOT)
                .contains("reject_retirement"));
        assertEquals(original, service().getSemanticModelByIdentifier(original.nameIdentifier()));
        assertTrue(backend.exists(target, type));
        assertTrue(backend.exists(source.nameIdentifier(), source.type()));
        assertEquals(initial, rows(original.id()));
        assertEquals(1, snapshotCount(original.id()));
      } finally {
        dropConstraint("reject_retirement");
      }
    }
  }

  @TestTemplate
  void testSchemaCascadeIncludesDescendantsAndPreservesOtherModels() throws Exception {
    SemanticModelEntity parent = model("parent", source.id(), "employee");
    backend.insert(parent, false);
    createAndInsertSchema("lake", "models", "semantic:child");
    Namespace originalNamespace = modelNamespace;
    modelNamespace = Namespace.of("lake", "models", "semantic:child");
    SemanticModelEntity child = model("child", source.id(), "manager");
    backend.insert(child, false);
    createAndInsertSchema("lake", "models", "other");
    modelNamespace = Namespace.of("lake", "models", "other");
    SemanticModelEntity other = model("other", source.id(), "employee");
    backend.insert(other, false);
    List<Association> unrelated = rows(other.id());
    assertTrue(
        backend.delete(
            NameIdentifier.of(originalNamespace.levels()), Entity.EntityType.SCHEMA, true));
    assertTrue(live(parent.id()).isEmpty());
    assertTrue(live(child.id()).isEmpty());
    assertEquals(unrelated, rows(other.id()));
  }

  @TestTemplate
  void testSourceDeletionAndParentDeletionPreserveLiveAssociations() throws Exception {
    for (Entity.EntityType type :
        List.of(Entity.EntityType.TABLE, Entity.EntityType.SCHEMA, Entity.EntityType.CATALOG)) {
      resetDatabase();
      prepareSources();
      SemanticModelEntity original = model("staff", source.id(), "employee");
      backend.insert(original, false);
      List<Association> initial = rows(original.id());
      NameIdentifier target =
          switch (type) {
            case TABLE -> source.nameIdentifier();
            case SCHEMA -> NameIdentifier.of(sourceNamespace.levels());
            default -> NameIdentifier.of("lake", "sources");
          };
      assertTrue(backend.delete(target, type, true));
      backend.hardDeleteLegacyData(type, Long.MAX_VALUE);
      service().deleteSemanticModelMetasByLegacyTimeline(Long.MAX_VALUE, 100);
      assertEquals(initial, rows(original.id()));
      assertEquals(original, service().getSemanticModelByIdentifier(original.nameIdentifier()));
    }
  }

  @TestTemplate
  void testGcTimelineLimitAndSnapshotRetention() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee", "manager");
    backend.insert(original, false);
    SemanticModelEntity replacement = model("staff", source.id(), "employee");
    backend.update(
        original.nameIdentifier(),
        original.type(),
        old ->
            copy(original, original.name(), replacement.definition(), replacement.sourceUpdate()));
    List<Association> current = live(original.id());
    execute("UPDATE semantic_model_source_rel SET deleted_at = 10 WHERE deleted_at > 0");
    assertEquals(0, service().deleteSemanticModelMetasByLegacyTimeline(10L, 1));
    assertEquals(1, service().deleteSemanticModelMetasByLegacyTimeline(11L, 1));
    assertEquals(2, rows(original.id()).size());
    assertEquals(1, service().deleteSemanticModelMetasByLegacyTimeline(11L, 1));
    assertEquals(1, service().deleteSemanticModelVersionsByRetentionCount(1L, 100));
    service().deleteSemanticModelMetasByLegacyTimeline(Long.MAX_VALUE, 100);
    assertEquals(current, rows(original.id()));
    assertEquals(1, snapshotCount(original.id()));
  }

  @TestTemplate
  void testGcAfterModelAndParentsHaveBeenPhysicallyRemoved() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee", "manager");
    backend.insert(original, false);
    backend.delete(NameIdentifier.of("lake"), Entity.EntityType.METALAKE, true);
    SessionUtils.doWithCommitAndFetchResult(
        SemanticModelMetaMapper.class,
        mapper -> mapper.deleteSemanticModelMetasByLegacyTimeline(Long.MAX_VALUE, 100));
    SchemaMetaService.getInstance().deleteSchemaMetasByLegacyTimeline(Long.MAX_VALUE, 100);
    CatalogMetaService.getInstance().deleteCatalogMetasByLegacyTimeline(Long.MAX_VALUE, 100);
    MetalakeMetaService.getInstance().deleteMetalakeMetasByLegacyTimeline(Long.MAX_VALUE, 100);
    assertEquals(2, rows(original.id()).size());
    execute("UPDATE semantic_model_source_rel SET deleted_at = 10 WHERE deleted_at > 0");
    assertEquals(1, service().deleteSemanticModelMetasByLegacyTimeline(11L, 1));
    assertEquals(1, rows(original.id()).size());
    assertEquals(1, service().deleteSemanticModelMetasByLegacyTimeline(11L, 1));
    assertTrue(rows(original.id()).isEmpty());
  }

  private static SemanticModelMetaService service() {
    return SemanticModelMetaService.getInstance();
  }

  private NameIdentifier deletionTarget(SemanticModelEntity model, Entity.EntityType type) {
    return switch (type) {
      case SEMANTIC_MODEL -> model.nameIdentifier();
      case SCHEMA -> NameIdentifier.of(modelNamespace.levels());
      case CATALOG -> NameIdentifier.of("lake", "models");
      default -> NameIdentifier.of("lake");
    };
  }

  private void addConstraint(String name, String condition) throws Exception {
    execute(
        "ALTER TABLE semantic_model_source_rel ADD CONSTRAINT "
            + name
            + " CHECK ("
            + condition
            + ")");
  }

  private void dropConstraint(String name) throws Exception {
    execute(
        "ALTER TABLE semantic_model_source_rel DROP "
            + ("mysql".equals(backendType) ? "CHECK " : "CONSTRAINT ")
            + name);
  }

  private void execute(String sql) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Statement statement = session.getConnection().createStatement()) {
      statement.execute(sql);
      session.commit();
    }
  }

  private List<Association> rows(long modelId) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT id, metalake_id, dataset_name, source_type, source_id, deleted_at"
                        + " FROM semantic_model_source_rel WHERE semantic_model_id = ? ORDER BY id")) {
      statement.setLong(1, modelId);
      try (ResultSet result = statement.executeQuery()) {
        List<Association> rows = new ArrayList<>();
        while (result.next()) {
          rows.add(
              new Association(
                  result.getLong(1),
                  result.getLong(2),
                  result.getString(3),
                  result.getString(4),
                  result.getLong(5),
                  result.getLong(6)));
        }
        return rows;
      }
    }
  }

  private List<Association> live(long modelId) throws Exception {
    return rows(modelId).stream().filter(row -> row.deletedAt() == 0).toList();
  }

  private Set<Long> liveIds(long modelId) throws Exception {
    return live(modelId).stream().map(Association::id).collect(Collectors.toSet());
  }

  private Set<String> datasetNames(long modelId) throws Exception {
    return live(modelId).stream().map(Association::datasetName).collect(Collectors.toSet());
  }

  private Set<SemanticModelSource> sourceSet(long modelId) throws Exception {
    return live(modelId).stream()
        .map(
            row ->
                new SemanticModelSource(
                    row.datasetName(), Entity.EntityType.valueOf(row.sourceType()), row.sourceId()))
        .collect(Collectors.toSet());
  }

  private int snapshotCount(long modelId) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT COUNT(*) FROM semantic_model_version_info WHERE semantic_model_id = ? AND deleted_at = 0")) {
      statement.setLong(1, modelId);
      try (ResultSet result = statement.executeQuery()) {
        result.next();
        return result.getInt(1);
      }
    }
  }

  private SemanticModelEntity model(String name, long sourceId, String... datasets) {
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(
                Arrays.stream(datasets)
                    .map(
                        dataset ->
                            Dataset.builder()
                                .withName(dataset)
                                .withSource(NameIdentifier.of("sources", "hr", "employees"))
                                .build())
                    .toArray(Dataset[]::new))
            .build();
    return SemanticModelEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(modelNamespace)
        .withDefinition(definition)
        .withProperties(Map.of())
        .withAuditInfo(AUDIT_INFO)
        .withSourceUpdate(
            SemanticModelSourceUpdate.replace(
                Arrays.stream(datasets)
                    .map(
                        dataset ->
                            new SemanticModelSource(dataset, Entity.EntityType.TABLE, sourceId))
                    .toList()))
        .build();
  }

  private SemanticModelEntity copy(
      SemanticModelEntity original,
      String name,
      SemanticModelDefinition definition,
      SemanticModelSourceUpdate update) {
    return SemanticModelEntity.builder()
        .withId(original.id())
        .withName(name)
        .withNamespace(original.namespace())
        .withDefinition(definition)
        .withComment("Updated comment")
        .withProperties(Map.of("updated", "true"))
        .withAuditInfo(AUDIT_INFO)
        .withSourceUpdate(update)
        .build();
  }

  @SuppressWarnings("UnusedVariable") // Error Prone does not recognize record component accessors.
  private record Association(
      long id,
      long metalakeId,
      String datasetName,
      String sourceType,
      long sourceId,
      long deletedAt) {}
}
