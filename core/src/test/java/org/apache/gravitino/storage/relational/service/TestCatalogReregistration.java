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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableVersionMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.po.ViewPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Verifies identity reuse after catalog deletion without restoring or moving governance metadata.
 */
public class TestCatalogReregistration extends TestJDBCBackend {
  private static final String METALAKE = "catalog_reregistration";

  private long metalakeId;
  private UserEntity owner;
  private RoleEntity role;
  private TagEntity tag;

  @BeforeEach
  void createGovernanceDefinitions() throws IOException {
    metalakeId = createAndInsertMakeLake(METALAKE).id();
    owner = createUserEntity(nextId(), NamespaceUtil.ofUser(METALAKE), "owner", AUDIT_INFO);
    backend.insert(owner, false);
    role =
        RoleEntity.builder()
            .withId(nextId())
            .withName("role")
            .withNamespace(NamespaceUtil.ofRole(METALAKE))
            .withAuditInfo(AUDIT_INFO)
            .withSecurableObjects(List.of())
            .build();
    backend.insert(role, false);
    tag = createAndInsertTagEntity("governed", "tag definition", METALAKE);
  }

  @TestTemplate
  void testCatalogReregistrationDoesNotRestoreGovernance() throws Exception {
    Registration original = createRegistration("source", null, true);
    Map<Long, String> originalObjects = new LinkedHashMap<>(original.objects());
    addOtherGovernedObjects(original, originalObjects);
    seedGovernance(originalObjects);
    assertGovernance(originalObjects, 1);

    backend.delete(original.catalog().nameIdentifier(), EntityType.CATALOG, true);
    assertGovernance(originalObjects, 0);
    Registration reimported = createRegistration("source", original, true);
    assertGovernance(reimported.objects(), 0);
    assertEquals(original.schema().id(), reimported.schema().id());
    assertEquals(original.table().id(), reimported.table().id());
    assertEquals(original.topic().id(), reimported.topic().id());
    assertEquals(original.view().id(), reimported.view().id());

    seedGovernance(reimported.objects());
    assertGovernance(reimported.objects(), 1);
    backend.delete(reimported.catalog().nameIdentifier(), EntityType.CATALOG, true);
    assertGovernance(reimported.objects(), 0);
    assertEquals(1, countLive("user_meta", "user_id", owner.id(), null, null));
    assertEquals(1, countLive("role_meta", "role_id", role.id(), null, null));
    assertEquals(1, countLive("tag_meta", "tag_id", tag.id(), null, null));
  }

  @TestTemplate
  void testCatalogDropDoesNotDeleteGovernanceOfIdsReusedInAnotherCatalog() throws Exception {
    Registration original = createRegistration("original", null, true);
    backend.delete(original.schema().nameIdentifier(), EntityType.SCHEMA, true);
    Registration current = createRegistration("current", original, true);
    seedGovernance(current.objects());

    backend.delete(original.catalog().nameIdentifier(), EntityType.CATALOG, true);
    assertGovernance(current.objects(), 1);
    assertLiveTableData(current.table().id());
    assertEquals(1, countLive("view_version_info", "view_id", current.view().id(), null, null));
    assertTrue(backend.exists(current.table().nameIdentifier(), EntityType.TABLE));
    backend.delete(current.catalog().nameIdentifier(), EntityType.CATALOG, true);
    assertGovernance(current.objects(), 0);
  }

  @TestTemplate
  void testSchemaDropDoesNotDeleteGovernanceOfIdsReusedInAnotherSchema() throws Exception {
    Registration original = createRegistration("original", null, true);
    backend.delete(original.table().nameIdentifier(), EntityType.TABLE, false);
    backend.delete(original.topic().nameIdentifier(), EntityType.TOPIC, false);
    backend.delete(original.view().nameIdentifier(), EntityType.VIEW, false);
    Registration current = createRegistration("current", original, false);
    seedGovernance(current.objects());

    backend.delete(original.schema().nameIdentifier(), EntityType.SCHEMA, true);
    assertGovernance(current.objects(), 1);
    assertLiveTableData(current.table().id());
    assertEquals(1, countLive("view_version_info", "view_id", current.view().id(), null, null));
    backend.delete(current.schema().nameIdentifier(), EntityType.SCHEMA, true);
    Map<Long, String> children = new LinkedHashMap<>(current.objects());
    children.remove(current.catalog().id());
    assertGovernance(children, 0);
  }

  @TestTemplate
  void testStaleDropsDoNotDeleteReregisteredEntitiesOrGovernance() throws Exception {
    Registration original = createRegistration("source", null, true);
    TablePO oldTable =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class,
            mapper -> mapper.selectTableMetaByIdForUpdate(original.table().id()));
    TopicPO oldTopic =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class, mapper -> mapper.selectTopicMetaById(original.topic().id()));
    ViewPO oldView =
        ViewMetaService.getInstance().getViewPOByIdentifier(original.view().nameIdentifier());
    backend.delete(original.catalog().nameIdentifier(), EntityType.CATALOG, true);
    Registration current = createRegistration("source", original, true);
    seedGovernance(current.objects());

    assertThrows(
        NoSuchEntityException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    TableMetaService.getInstance()
                        .deleteTableWithVersion(original.table().nameIdentifier(), oldTable)));
    assertThrows(
        NoSuchEntityException.class,
        () ->
            TopicMetaService.getInstance()
                .deleteTopicWithVersion(original.topic().nameIdentifier(), oldTopic));
    assertThrows(
        NoSuchEntityException.class,
        () ->
            ViewMetaService.getInstance()
                .deleteViewWithVersion(original.view().nameIdentifier(), oldView));
    assertGovernance(current.objects(), 1);
    assertTrue(backend.exists(current.table().nameIdentifier(), EntityType.TABLE));
    assertTrue(backend.exists(current.topic().nameIdentifier(), EntityType.TOPIC));
    assertTrue(backend.exists(current.view().nameIdentifier(), EntityType.VIEW));
  }

  @TestTemplate
  void testStaleSchemaAlterDoesNotUpdateReregisteredSchema() throws Exception {
    Registration original = createRegistration("source", null, true);
    AtomicReference<Registration> replacement = new AtomicReference<>();
    assertThrows(
        NoSuchEntityException.class,
        () ->
            SchemaMetaService.getInstance()
                .updateSchema(
                    original.schema().nameIdentifier(),
                    entity -> {
                      Registration current =
                          assertDoesNotThrow(
                              () -> {
                                backend.delete(
                                    original.catalog().nameIdentifier(), EntityType.CATALOG, true);
                                Registration registered =
                                    createRegistration("source", original, true);
                                seedGovernance(registered.objects());
                                return registered;
                              });
                      replacement.set(current);
                      return entity;
                    }));
    assertGovernance(replacement.get().objects(), 1);
    assertTrue(backend.exists(replacement.get().schema().nameIdentifier(), EntityType.SCHEMA));
  }

  @TestTemplate
  void testMetalakeDropKeepsVersionsOfIdsReusedInAnotherMetalake() throws Exception {
    Registration original = createRegistration("original", null, true);
    backend.delete(original.schema().nameIdentifier(), EntityType.SCHEMA, true);
    String otherMetalake = "other_metalake";
    createAndInsertMakeLake(otherMetalake);
    createAndInsertCatalog(otherMetalake, "current");
    SchemaEntity schema =
        createSchemaEntity(
            original.schema().id(), Namespace.of(otherMetalake, "current"), "schema", AUDIT_INFO);
    backend.insert(schema, false);
    TableEntity table =
        TableEntity.builder()
            .withId(original.table().id())
            .withName("table")
            .withNamespace(Namespace.of(otherMetalake, "current", schema.name()))
            .withColumns(List.of(original.column()))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(table, false);
    ViewEntity view = createViewEntity(original.view().id(), table.namespace(), "view");
    backend.insert(view, false);

    backend.delete(NameIdentifier.of(METALAKE), EntityType.METALAKE, true);
    assertTrue(backend.exists(table.nameIdentifier(), EntityType.TABLE));
    assertLiveTableData(table.id());
    assertEquals(1, countLive("view_version_info", "view_id", view.id(), null, null));
  }

  @TestTemplate
  void testLegacyGcPreservesActiveRegistrationsAndTheirChildren() throws Exception {
    Registration current = createRegistration("source", null, true);
    for (int generation = 0; generation < 2; generation++) {
      backend.delete(current.catalog().nameIdentifier(), EntityType.CATALOG, true);
      current = createRegistration("source", current, true);
    }
    seedGovernance(current.objects());
    Map<String, Long> activeIds =
        Map.of(
            "schema",
            current.schema().id(),
            "table",
            current.table().id(),
            "topic",
            current.topic().id(),
            "view",
            current.view().id());
    Map<String, IntSupplier> rootGc =
        Map.of(
            "schema",
                () ->
                    SessionUtils.doWithCommitAndFetchResult(
                        SchemaMetaMapper.class,
                        mapper -> mapper.deleteSchemaMetasByLegacyTimeline(Long.MAX_VALUE, 1)),
            "table",
                () ->
                    SessionUtils.doWithCommitAndFetchResult(
                        TableMetaMapper.class,
                        mapper -> mapper.deleteTableMetasByLegacyTimeline(Long.MAX_VALUE, 1)),
            "topic",
                () ->
                    SessionUtils.doWithCommitAndFetchResult(
                        TopicMetaMapper.class,
                        mapper -> mapper.deleteTopicMetasByLegacyTimeline(Long.MAX_VALUE, 1)),
            "view",
                () ->
                    SessionUtils.doWithCommitAndFetchResult(
                        ViewMetaMapper.class,
                        mapper -> mapper.deleteViewMetasByLegacyTimeline(Long.MAX_VALUE, 1)));
    for (Map.Entry<String, IntSupplier> gc : rootGc.entrySet()) {
      assertEquals(1, gc.getValue().getAsInt(), gc.getKey() + " first GC batch");
      assertEquals(1, gc.getValue().getAsInt(), gc.getKey() + " second GC batch");
      assertEquals(0, gc.getValue().getAsInt(), gc.getKey() + " exhausted history");
      assertEquals(
          1,
          countLive(
              gc.getKey() + "_meta", gc.getKey() + "_id", activeIds.get(gc.getKey()), null, null));
    }
    for (int generation = 0; generation < 2; generation++) {
      assertEquals(
          1,
          SessionUtils.doWithCommitAndFetchResult(
                  TableVersionMapper.class,
                  mapper -> mapper.deleteTableVersionByLegacyTimeline(Long.MAX_VALUE, 1))
              .intValue());
      assertEquals(
          1,
          SessionUtils.doWithCommitAndFetchResult(
                  TableColumnMapper.class,
                  mapper -> mapper.deleteColumnPOsByLegacyTimeline(Long.MAX_VALUE, 1))
              .intValue());
      assertEquals(
          1,
          SessionUtils.doWithCommitAndFetchResult(
                  ViewVersionInfoMapper.class,
                  mapper -> mapper.deleteViewVersionsByLegacyTimeline(Long.MAX_VALUE, 1))
              .intValue());
    }
    assertGovernance(current.objects(), 1);
    assertLiveTableData(current.table().id());
    assertTrue(backend.exists(current.schema().nameIdentifier(), EntityType.SCHEMA));
    assertTrue(backend.exists(current.table().nameIdentifier(), EntityType.TABLE));
    assertTrue(backend.exists(current.topic().nameIdentifier(), EntityType.TOPIC));
    assertTrue(backend.exists(current.view().nameIdentifier(), EntityType.VIEW));
    assertEquals(1, countLive("view_version_info", "view_id", current.view().id(), null, null));
  }

  private Registration createRegistration(
      String name, @Nullable Registration previous, boolean reuseSchemaId) throws IOException {
    CatalogEntity catalog = createAndInsertCatalog(METALAKE, name);
    Namespace schemaNamespace = Namespace.of(METALAKE, name);
    SchemaEntity schema =
        createSchemaEntity(
            previous != null && reuseSchemaId ? previous.schema().id() : nextId(),
            schemaNamespace,
            "schema",
            AUDIT_INFO);
    backend.insert(schema, false);
    Namespace namespace = Namespace.of(METALAKE, name, schema.name());
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(previous == null ? nextId() : previous.column().id())
            .withName("column")
            .withPosition(0)
            .withDataType(Types.IntegerType.get())
            .withNullable(false)
            .withAutoIncrement(false)
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity table =
        TableEntity.builder()
            .withId(previous == null ? nextId() : previous.table().id())
            .withName("table")
            .withNamespace(namespace)
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TopicEntity topic =
        createTopicEntity(
            previous == null ? nextId() : previous.topic().id(), namespace, "topic", AUDIT_INFO);
    ViewEntity view =
        createViewEntity(previous == null ? nextId() : previous.view().id(), namespace, "view");
    backend.insert(table, false);
    backend.insert(topic, false);
    backend.insert(view, false);
    return new Registration(catalog, schema, table, column, topic, view);
  }

  private void addOtherGovernedObjects(Registration registration, Map<Long, String> objects)
      throws IOException {
    Namespace namespace = registration.table().namespace();
    FilesetEntity fileset = createFilesetEntity(nextId(), namespace, "fileset", AUDIT_INFO);
    ModelEntity model =
        createModelEntity(nextId(), namespace, "model", "comment", 1, null, AUDIT_INFO);
    FunctionEntity function = createFunctionEntity(nextId(), namespace, "function", AUDIT_INFO);
    SemanticModelEntity semantic =
        SemanticModelEntity.builder()
            .withId(nextId())
            .withName("semantic")
            .withNamespace(namespace)
            .withAuditInfo(AUDIT_INFO)
            .withDefinition(
                SemanticModelDefinition.builder()
                    .withDatasets(
                        new Dataset[] {
                          Dataset.builder()
                              .withName("source")
                              .withSource(NameIdentifier.of("source", "schema", "table"))
                              .build()
                        })
                    .build())
            .build();
    backend.insert(fileset, false);
    backend.insert(model, false);
    backend.insert(function, false);
    backend.insert(semantic, false);
    objects.put(fileset.id(), "FILESET");
    objects.put(model.id(), "MODEL");
    objects.put(function.id(), "FUNCTION");
    objects.put(semantic.id(), "SEMANTIC_MODEL");
  }

  private void seedGovernance(Map<Long, String> objects) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      for (Map.Entry<Long, String> object : objects.entrySet()) {
        long id = object.getKey();
        String type = object.getValue();
        statement.executeUpdate(
            String.format(
                "INSERT INTO owner_meta (metalake_id, owner_id, owner_type, metadata_object_id, metadata_object_type, audit_info) VALUES (%d, %d, 'USER', %d, '%s', '{}')",
                metalakeId, owner.id(), id, type));
        statement.executeUpdate(
            String.format(
                "INSERT INTO role_meta_securable_object (role_id, metadata_object_id, type, privilege_names, privilege_conditions) VALUES (%d, %d, '%s', '[]', '[]')",
                role.id(), id, type));
        statement.executeUpdate(
            String.format(
                "INSERT INTO tag_relation_meta (tag_id, metadata_object_id, metadata_object_type, audit_info) VALUES (%d, %d, '%s', '{}')",
                tag.id(), id, type));
        statement.executeUpdate(
            String.format(
                "INSERT INTO statistic_meta (statistic_id, statistic_name, metalake_id, statistic_value, metadata_object_id, metadata_object_type, audit_info) VALUES (%d, 'row_count', %d, '{}', %d, '%s', '{}')",
                nextId(), metalakeId, id, type));
      }
    }
  }

  private void assertLiveTableData(long tableId) throws SQLException {
    assertEquals(
        1,
        countLive("table_version_info", "table_id", tableId, null, null),
        "The active table version must survive cleanup of a previous registration");
    assertEquals(
        1,
        countLive("table_column_version_info", "table_id", tableId, null, null),
        "The active table columns must survive cleanup of a previous registration");
  }

  private void assertGovernance(Map<Long, String> objects, int expected) throws SQLException {
    for (Map.Entry<Long, String> object : objects.entrySet()) {
      for (String table :
          List.of(
              "owner_meta", "role_meta_securable_object", "tag_relation_meta", "statistic_meta")) {
        String typeColumn =
            table.equals("role_meta_securable_object") ? "type" : "metadata_object_type";
        assertEquals(
            expected,
            countLive(table, "metadata_object_id", object.getKey(), typeColumn, object.getValue()),
            table + " for " + object.getValue() + " " + object.getKey());
      }
    }
  }

  private int countLive(
      String table, String idColumn, long id, @Nullable String typeColumn, @Nullable String type)
      throws SQLException {
    String sql =
        "SELECT COUNT(*) FROM "
            + table
            + " WHERE "
            + idColumn
            + " = ? AND deleted_at = 0"
            + (typeColumn == null ? "" : " AND " + typeColumn + " = ?");
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, id);
      if (typeColumn != null) {
        statement.setString(2, type);
      }
      try (ResultSet result = statement.executeQuery()) {
        assertTrue(result.next());
        return result.getInt(1);
      }
    }
  }

  private static long nextId() {
    return RandomIdGenerator.INSTANCE.nextId();
  }

  @SuppressWarnings("UnusedVariable") // Error Prone does not recognize record component accessors.
  private record Registration(
      CatalogEntity catalog,
      SchemaEntity schema,
      TableEntity table,
      ColumnEntity column,
      TopicEntity topic,
      ViewEntity view) {
    private Map<Long, String> objects() {
      return Map.of(
          catalog.id(),
          "CATALOG",
          schema.id(),
          "SCHEMA",
          table.id(),
          "TABLE",
          column.id(),
          "COLUMN",
          topic.id(),
          "TOPIC",
          view.id(),
          "VIEW");
    }
  }
}
