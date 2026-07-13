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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCatalogMetaService extends TestJDBCBackend {

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
  private final String metalakeName = "metalake_for_catalog_test";

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(catalogCopy, false));
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    CatalogEntity catalogCopy =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog1",
            auditInfo);
    backend.insert(catalog, false);
    backend.insert(catalogCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                catalogCopy.nameIdentifier(),
                Entity.EntityType.CATALOG,
                e ->
                    createCatalog(
                        catalogCopy.id(), catalogCopy.namespace(), "catalog", auditInfo)));
  }

  @TestTemplate
  void testUpdateCatalogWithNullableComment() throws IOException {
    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withNamespace(NamespaceUtil.ofCatalog(metalakeName))
            .withName("catalog")
            .withAuditInfo(auditInfo)
            .withComment(null)
            .withProperties(null)
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .build();
    backend.insert(catalog, false);

    backend.update(
        catalog.nameIdentifier(),
        Entity.EntityType.CATALOG,
        e ->
            CatalogEntity.builder()
                .withId(catalog.id())
                .withNamespace(catalog.namespace())
                .withName(catalog.name())
                .withAuditInfo(auditInfo)
                .withComment("comment")
                .withProperties(catalog.getProperties())
                .withType(Catalog.Type.RELATIONAL)
                .withProvider("test")
                .build());

    CatalogEntity updatedCatalog = backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    Assertions.assertNotNull(updatedCatalog.getComment());
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog",
            auditInfo);
    backend.insert(catalog, false);

    List<CatalogEntity> catalogs =
        backend.list(catalog.namespace(), Entity.EntityType.CATALOG, true);
    assertTrue(catalogs.contains(catalog));
    assertEquals(1, catalogs.size());

    CatalogEntity catalogEntity = backend.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    assertEquals(catalog, catalogEntity);
    Assertions.assertNotNull(
        CatalogMetaService.getInstance()
            .getCatalogPOByName(catalogEntity.namespace().level(0), catalog.name()));
    assertEquals(
        catalog.id(),
        CatalogMetaService.getInstance()
            .getCatalogIdByName(catalog.namespace().level(0), catalog.name()));

    // meta data soft delete
    backend.delete(NameIdentifierUtil.ofMetalake(metalakeName), Entity.EntityType.METALAKE, true);

    assertEquals(
        0,
        SessionUtils.doWithCommitAndFetchResult(
                CatalogMetaMapper.class,
                mapper -> mapper.listCatalogPOsByMetalakeName(metalakeName))
            .size());

    // check existence after soft delete
    assertFalse(backend.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));

    // meta data hard delete
    backend.hardDeleteLegacyData(Entity.EntityType.CATALOG, Instant.now().toEpochMilli() + 3000);
    assertFalse(legacyRecordExistsInDB(catalog.id(), Entity.EntityType.CATALOG));
  }

  @TestTemplate
  public void testDeleteCatalogCascadeRemovesTagRelations() throws IOException {
    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            "catalog_with_tags",
            auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalog.name()),
            "schema_with_tags",
            AUDIT_INFO);
    backend.insert(schema, false);

    Namespace objectNamespace = Namespace.of(metalakeName, catalog.name(), schema.name());
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column_with_tag")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table_with_tag")
            .withNamespace(objectNamespace)
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(table, false);
    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "topic_with_tag", AUDIT_INFO);
    TopicMetaService.getInstance().insertTopic(topic, false);
    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "fileset_with_tag", AUDIT_INFO);
    FilesetMetaService.getInstance().insertFileset(fileset, false);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            objectNamespace,
            "model_with_tag",
            "comment",
            1,
            null,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ViewEntity view =
        createViewEntity(RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "view_with_tag");
    FunctionEntity function =
        createFunctionEntity(
            RandomIdGenerator.INSTANCE.nextId(), objectNamespace, "function_with_tag", AUDIT_INFO);
    ViewMetaService.getInstance().insertView(view, false);
    FunctionMetaService.getInstance().insertFunction(function, false);

    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService.getInstance().insertTag(tag, false);
    associateTag(tag, catalog.nameIdentifier(), catalog.type());
    associateTag(tag, schema.nameIdentifier(), schema.type());
    associateTag(tag, table.nameIdentifier(), table.type());
    associateTag(
        tag,
        NameIdentifier.of(Namespace.fromString(table.nameIdentifier().toString()), column.name()),
        column.type());
    associateTag(tag, topic.nameIdentifier(), topic.type());
    associateTag(tag, fileset.nameIdentifier(), fileset.type());
    associateTag(tag, model.nameIdentifier(), model.type());
    associateTag(tag, view.nameIdentifier(), view.type());
    associateTag(tag, function.nameIdentifier(), function.type());

    assertEquals(1, countActiveTagRelForMetadataObject(catalog.id(), "CATALOG"));
    assertEquals(1, countActiveTagRelForMetadataObject(schema.id(), "SCHEMA"));
    assertEquals(1, countActiveTagRelForMetadataObject(table.id(), "TABLE"));
    assertEquals(1, countActiveTagRelForMetadataObject(column.id(), "COLUMN"));
    assertEquals(1, countActiveTagRelForMetadataObject(topic.id(), "TOPIC"));
    assertEquals(1, countActiveTagRelForMetadataObject(fileset.id(), "FILESET"));
    assertEquals(1, countActiveTagRelForMetadataObject(model.id(), "MODEL"));
    assertEquals(1, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
    assertEquals(1, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));

    assertTrue(CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), true));

    assertEquals(0, countActiveTagRelForMetadataObject(catalog.id(), "CATALOG"));
    assertEquals(0, countActiveTagRelForMetadataObject(schema.id(), "SCHEMA"));
    assertEquals(0, countActiveTagRelForMetadataObject(table.id(), "TABLE"));
    assertEquals(0, countActiveTagRelForMetadataObject(column.id(), "COLUMN"));
    assertEquals(0, countActiveTagRelForMetadataObject(topic.id(), "TOPIC"));
    assertEquals(0, countActiveTagRelForMetadataObject(fileset.id(), "FILESET"));
    assertEquals(0, countActiveTagRelForMetadataObject(model.id(), "MODEL"));
    assertEquals(0, countActiveTagRelForMetadataObject(view.id(), "VIEW"));
    assertEquals(0, countActiveTagRelForMetadataObject(function.id(), "FUNCTION"));
  }

  private void associateTag(TagEntity tag, NameIdentifier ident, Entity.EntityType type)
      throws IOException {
    TagMetaService.getInstance()
        .associateTagsWithMetadataObject(
            ident,
            type,
            new NameIdentifier[] {NameIdentifierUtil.ofTag(metalakeName, tag.name())},
            new NameIdentifier[0]);
  }

  private int countActiveTagRelForMetadataObject(Long metadataObjectId, String metadataObjectType) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM tag_relation_meta"
                        + " WHERE metadata_object_id = %d AND metadata_object_type = '%s'"
                        + " AND deleted_at = 0",
                    metadataObjectId, metadataObjectType))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("No result for countActiveTagRelForMetadataObject");
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }
}
