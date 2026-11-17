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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationQuery;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.SemanticModelDatasetListing;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelSource;
import org.apache.gravitino.semantic.SemanticModelSourceUpdate;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SemanticModelSourceMapper;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Exercises indexed source lookup and lifecycle visibility on all relational backends. */
public class TestSemanticModelSourceService extends TestJDBCBackend {
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
  void testCrossCatalogSharedSourcesCaseAndLongNames() throws Exception {
    String[] names = {"employee", "Employee", "manager.with.dot", "long".repeat(300), "员工"};
    SemanticModelEntity model = model("staff", source.id(), names);
    backend.insert(model, false);
    List<SemanticModelDatasetListing> found = lookup(source.nameIdentifier(), source.type());
    assertEquals(1, found.size());
    assertEquals(model.id(), found.get(0).id());
    assertEquals(model.nameIdentifier(), found.get(0).nameIdentifier());
    assertEquals(Set.of(names), Set.copyOf(found.get(0).datasets()));
    assertEquals(names.length, rows(source.id(), source.type()).size());
    assertTrue(
        lookup(
                createAndInsertTableEntity(sourceNamespace, "unused").nameIdentifier(),
                source.type())
            .isEmpty());
    assertThrows(
        NoSuchEntityException.class,
        () -> lookup(NameIdentifier.of(sourceNamespace, "missing"), source.type()));
    assertThrows(
        IllegalArgumentException.class,
        () -> lookup(source.nameIdentifier(), Entity.EntityType.SCHEMA));
  }

  @TestTemplate
  void testMultipleModelsAndDuplicateRowsPreserveExactDatasetNames() throws Exception {
    SemanticModelEntity first = model("staff", source.id(), "employee", "Employee");
    SemanticModelEntity second = model("staff_again", source.id(), "employee");
    backend.insert(first, false);
    backend.insert(second, false);
    SessionUtils.doWithCommit(
        SemanticModelSourceMapper.class,
        mapper ->
            mapper.insertSources(
                metalakeId,
                first.id(),
                List.of(new SemanticModelSource("employee", source.type(), source.id()))));
    Map<Long, List<String>> found =
        lookup(source.nameIdentifier(), source.type()).stream()
            .collect(
                Collectors.toMap(
                    SemanticModelDatasetListing::id, SemanticModelDatasetListing::datasets));
    assertEquals(2, found.size());
    assertEquals(2, found.get(first.id()).size());
    assertEquals(Set.of("employee", "Employee"), Set.copyOf(found.get(first.id())));
    assertEquals(List.of("employee"), found.get(second.id()));
  }

  @TestTemplate
  void testCurrentParentNamesAndHierarchicalSchema() throws Exception {
    SemanticModelEntity model = model("staff", source.id(), "employee");
    backend.insert(model, false);
    String physicalSchema =
        HierarchicalSchemaUtil.logicalToPhysical(
            "parent.child", HierarchicalSchemaUtil.schemaSeparator());
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement schema =
            session
                .getConnection()
                .prepareStatement(
                    "UPDATE schema_meta SET schema_name = ? WHERE schema_name = 'semantic'");
        Statement catalog = session.getConnection().createStatement()) {
      schema.setString(1, physicalSchema);
      schema.executeUpdate();
      catalog.executeUpdate(
          "UPDATE catalog_meta SET catalog_name = 'models_v2' WHERE catalog_name = 'models'");
      session.commit(true);
    }
    assertEquals(
        NameIdentifier.of("lake", "models_v2", "parent.child", "staff"),
        lookup(source.nameIdentifier(), source.type()).get(0).nameIdentifier());
  }

  @TestTemplate
  void testDeletedModelsAndParentsAreExcludedEvenWithLiveAssociations() throws Exception {
    backend.insert(model("staff", source.id(), "employee"), false);
    for (String table :
        List.of("semantic_model_meta", "schema_meta", "catalog_meta", "metalake_meta")) {
      try (SqlSession session =
              SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
          Statement statement = session.getConnection().createStatement()) {
        statement.executeUpdate("UPDATE " + table + " SET deleted_at = 1");
        session.commit(true);
        try {
          assertEquals(1, associationCount());
          assertTrue(rows(source.id(), source.type()).isEmpty(), table);
        } finally {
          statement.executeUpdate("UPDATE " + table + " SET deleted_at = 0");
          session.commit(true);
        }
      }
    }
    assertEquals(1, lookup(source.nameIdentifier(), source.type()).size());
  }

  @TestTemplate
  void testViewAndTableTypesAreDistinct() throws Exception {
    ViewEntity view = createViewEntity(source.id(), sourceNamespace, "employees_view");
    backend.insert(view, false);
    SemanticModelEntity tableModel = model("table_model", source.id(), "employee");
    backend.insert(tableModel, false);
    SemanticModelEntity viewModel =
        copy(
            tableModel,
            "view_model",
            tableModel.definition(),
            SemanticModelSourceUpdate.replace(
                List.of(new SemanticModelSource("employee", Entity.EntityType.VIEW, view.id()))),
            RandomIdGenerator.INSTANCE.nextId());
    backend.insert(viewModel, false);
    assertEquals(tableModel.id(), lookup(source.nameIdentifier(), source.type()).get(0).id());
    assertEquals(viewModel.id(), lookup(view.nameIdentifier(), view.type()).get(0).id());
  }

  @TestTemplate
  void testSourceRenameAndMetadataUpdatesPreserveBinding() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee", "manager");
    backend.insert(original, false);
    TableEntity renamed =
        createTableEntity(source.id(), sourceNamespace, "employees_v2", AUDIT_INFO);
    backend.update(source.nameIdentifier(), source.type(), old -> renamed);
    SemanticModelEntity renamedModel =
        copy(
            original,
            "staff_v2",
            original.definition(),
            SemanticModelSourceUpdate.preserve(),
            original.id());
    backend.update(original.nameIdentifier(), original.type(), old -> renamedModel);
    assertEquals(
        renamedModel.nameIdentifier(),
        lookup(renamed.nameIdentifier(), renamed.type()).get(0).nameIdentifier());
    SemanticModelEntity loaded =
        SemanticModelMetaService.getInstance()
            .getSemanticModelByIdentifier(renamedModel.nameIdentifier());
    assertEquals(
        NameIdentifier.of("sources", "hr", "employees"),
        loaded.definition().datasets()[0].source());
    assertEquals(2, rows(source.id(), source.type()).size());
  }

  @TestTemplate
  void testDeletionNameReuseAndIdenticalReplacement() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee");
    backend.insert(original, false);
    backend.delete(source.nameIdentifier(), source.type(), false);
    assertEquals(1, rows(source.id(), source.type()).size());
    assertThrows(NoSuchEntityException.class, () -> lookup(source.nameIdentifier(), source.type()));
    TableEntity recreated = createAndInsertTableEntity(sourceNamespace, source.name());
    assertTrue(lookup(recreated.nameIdentifier(), recreated.type()).isEmpty());
    SemanticModelEntity metadataOnly =
        copy(
            original,
            "staff",
            original.definition(),
            SemanticModelSourceUpdate.preserve(),
            original.id());
    backend.update(original.nameIdentifier(), original.type(), old -> metadataOnly);
    assertEquals(1, rows(source.id(), source.type()).size());
    SemanticModelEntity replacement =
        copy(
            original,
            "staff",
            original.definition(),
            SemanticModelSourceUpdate.replace(
                List.of(new SemanticModelSource("employee", source.type(), recreated.id()))),
            original.id());
    backend.update(original.nameIdentifier(), original.type(), old -> replacement);
    assertTrue(rows(source.id(), source.type()).isEmpty());
    assertEquals(
        List.of("employee"),
        lookup(recreated.nameIdentifier(), recreated.type()).get(0).datasets());
  }

  @TestTemplate
  void testDatasetReplacementAndOverwriteUsePersistedModelIdentity() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee", "manager");
    backend.insert(original, false);
    SemanticModelEntity replacement = model("staff", source.id(), "renamed");
    backend.insert(replacement, true);
    List<SemanticModelDatasetListing> found = lookup(source.nameIdentifier(), source.type());
    assertEquals(1, found.size());
    assertEquals(original.id(), found.get(0).id());
    assertEquals(List.of("renamed"), found.get(0).datasets());
    backend.delete(original.nameIdentifier(), original.type(), false);
    assertEquals(0, associationCount());
    assertTrue(rows(source.id(), source.type()).isEmpty());
  }

  @TestTemplate
  void testInsertFailureRestoresDeletedAssociationsAndDefinition() throws Exception {
    SemanticModelEntity original = model("staff", source.id(), "employee");
    backend.insert(original, false);
    TableEntity blocked = createAndInsertTableEntity(sourceNamespace, "blocked");
    // Reject the new association inside the database, after the old associations were retired.
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Statement statement = session.getConnection().createStatement()) {
      statement.execute(
          "ALTER TABLE semantic_model_source_rel ADD CONSTRAINT reject_test_source CHECK (source_id <> "
              + blocked.id()
              + ")");
      session.commit(true);
    }
    try {
      SemanticModelEntity definition = model("staff", blocked.id(), "renamed");
      SemanticModelEntity replacement =
          copy(
              original,
              original.name(),
              definition.definition(),
              definition.sourceUpdate(),
              original.id());
      IOException failure =
          assertThrows(
              IOException.class,
              () -> backend.update(original.nameIdentifier(), original.type(), old -> replacement));
      assertTrue(
          ExceptionUtils.getStackTrace(failure)
              .toLowerCase(Locale.ROOT)
              .contains("reject_test_source"));
      assertEquals(
          original,
          SemanticModelMetaService.getInstance()
              .getSemanticModelByIdentifier(original.nameIdentifier()));
      assertEquals(1, associationCount());
      assertEquals(
          List.of("employee"), lookup(source.nameIdentifier(), source.type()).get(0).datasets());
      assertTrue(lookup(blocked.nameIdentifier(), blocked.type()).isEmpty());
    } finally {
      try (SqlSession session =
              SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
          Statement statement = session.getConnection().createStatement()) {
        statement.execute(
            "ALTER TABLE semantic_model_source_rel DROP "
                + ("mysql".equals(backendType) ? "CHECK" : "CONSTRAINT")
                + " reject_test_source");
        session.commit(true);
      }
    }
  }

  @TestTemplate
  void testSourceParentDeletionKeepsAssociations() throws Exception {
    SemanticModelEntity model = model("staff", source.id(), "employee");
    backend.insert(model, false);
    backend.delete(NameIdentifier.of("lake", "sources"), Entity.EntityType.CATALOG, true);
    assertEquals(1, associationCount());
    assertEquals(1, rows(source.id(), source.type()).size());
    assertEquals(
        model,
        SemanticModelMetaService.getInstance()
            .getSemanticModelByIdentifier(model.nameIdentifier()));
    backend.delete(NameIdentifier.of(modelNamespace.levels()), Entity.EntityType.SCHEMA, true);
    assertEquals(0, associationCount());
    assertTrue(rows(source.id(), source.type()).isEmpty());
  }

  @TestTemplate
  void testModelParentDeletionCleansRows() throws Exception {
    SemanticModelEntity model = model("staff", source.id(), "employee");
    backend.insert(model, false);
    backend.delete(NameIdentifier.of("lake", "models"), Entity.EntityType.CATALOG, true);
    assertEquals(0, associationCount());
    assertTrue(rows(source.id(), source.type()).isEmpty());
    createAndInsertCatalog("lake", "models");
    createAndInsertSchema("lake", "models", "semantic");
    backend.insert(model("staff", source.id(), "employee"), false);
    backend.delete(NameIdentifier.of("lake"), Entity.EntityType.METALAKE, true);
    assertEquals(0, associationCount());
    assertTrue(rows(source.id(), source.type()).isEmpty());
  }

  private int associationCount() throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "SELECT COUNT(*) FROM semantic_model_source_rel WHERE metalake_id = ? AND deleted_at = 0")) {
      statement.setLong(1, metalakeId);
      try (ResultSet result = statement.executeQuery()) {
        result.next();
        return result.getInt(1);
      }
    }
  }

  private List<SemanticModelDatasetListing> lookup(
      NameIdentifier identifier, Entity.EntityType type) throws Exception {
    return backend.listEntitiesByRelation(
        RelationQuery.of(
            SupportsRelationOperations.Type.SEMANTIC_MODEL_SOURCE_REL,
            identifier,
            type,
            false,
            null));
  }

  private List<?> rows(long sourceId, Entity.EntityType type) {
    return SessionUtils.getWithoutCommit(
        SemanticModelSourceMapper.class,
        mapper -> mapper.listForSource(metalakeId, type.name(), sourceId));
  }

  private SemanticModelEntity model(String name, long sourceId, String... names) {
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(
                Arrays.stream(names)
                    .map(
                        dataset ->
                            Dataset.builder()
                                .withName(dataset)
                                .withSource(NameIdentifier.of("sources", "hr", "employees"))
                                .build())
                    .toArray(Dataset[]::new))
            .build();
    SemanticModelSourceUpdate update =
        SemanticModelSourceUpdate.replace(
            Arrays.stream(names)
                .map(dataset -> new SemanticModelSource(dataset, Entity.EntityType.TABLE, sourceId))
                .collect(Collectors.toList()));
    return SemanticModelEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(modelNamespace)
        .withDefinition(definition)
        .withProperties(Map.of())
        .withAuditInfo(AUDIT_INFO)
        .withSourceUpdate(update)
        .build();
  }

  private SemanticModelEntity copy(
      SemanticModelEntity original,
      String name,
      SemanticModelDefinition definition,
      SemanticModelSourceUpdate update,
      long id) {
    return SemanticModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(original.namespace())
        .withDefinition(definition)
        .withProperties(Map.of("updated", "true"))
        .withAuditInfo(AUDIT_INFO)
        .withSourceUpdate(update)
        .build();
  }
}
