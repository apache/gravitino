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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationEdgeTarget;
import org.apache.gravitino.RelationQuery;
import org.apache.gravitino.RelationUpdate;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.tag.TagValueConstraint;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestTagMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_tag_meta_service_test";

  private final Map<String, String> props = ImmutableMap.of("k1", "v1");

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    String anotherMetalakeName = METALAKE_NAME + "_another";
    BaseMetalake anotherMetaLake = createAndInsertMakeLake(anotherMetalakeName);

    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag")
            .withNamespace(NamespaceUtil.ofTag(metalake.name()))
            .withComment("tag comment")
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(tag, false);

    TagEntity anotherTagEntity =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("another-tag")
            .withNamespace(NamespaceUtil.ofTag(anotherMetaLake.name()))
            .withComment("another-tag comment")
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(anotherTagEntity, false);

    TagEntity tagEntity = backend.get(tag.nameIdentifier(), Entity.EntityType.TAG);
    assertEquals(tag, tagEntity);
    List<TagEntity> tags = backend.list(tag.namespace(), Entity.EntityType.TAG, true);
    assertTrue(tags.contains(tag));
    assertEquals(1, tags.size());

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);

    // check existence after soft delete
    assertFalse(backend.exists(tag.nameIdentifier(), Entity.EntityType.TAG));
    assertTrue(backend.exists(anotherTagEntity.nameIdentifier(), Entity.EntityType.TAG));
    assertTrue(legacyRecordExistsInDB(tag.id(), Entity.EntityType.TAG));

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(tag.id(), Entity.EntityType.TAG));
  }

  @TestTemplate
  public void testInsertAndGetTagByIdentifier() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    // Test no tag entity.
    TagMetaService tagMetaService = TagMetaService.getInstance();
    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1")));
    Assertions.assertEquals("No such tag entity: tag1", excep.getMessage());

    // Test get tag entity
    TagEntity tagEntity =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity, false);

    TagEntity resultTagEntity =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1"));
    Assertions.assertEquals(tagEntity, resultTagEntity);

    // Test with null comment and properties.
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withAuditInfo(AUDIT_INFO)
            .build();

    tagMetaService.insertTag(tagEntity1, false);
    TagEntity resultTagEntity1 =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"));
    Assertions.assertEquals(tagEntity1, resultTagEntity1);
    Assertions.assertNull(resultTagEntity1.comment());
    Assertions.assertNull(resultTagEntity1.properties());

    // Test insert with overwrite.
    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(tagEntity1.id())
            .withName("tag3")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();

    Assertions.assertThrows(Exception.class, () -> tagMetaService.insertTag(tagEntity2, false));

    tagMetaService.insertTag(tagEntity2, true);

    TagEntity resultTagEntity2 =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag3"));
    Assertions.assertEquals(tagEntity2, resultTagEntity2);
  }

  @TestTemplate
  public void testUpdateTagCommentFromNull() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag_null_comment")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity, false);

    tagMetaService.updateTag(
        tagEntity.nameIdentifier(),
        entity -> {
          TagEntity tag = (TagEntity) entity;
          return TagEntity.builder()
              .withId(tag.id())
              .withName(tag.name())
              .withNamespace(tag.namespace())
              .withComment("updated tag comment")
              .withProperties(tag.properties())
              .withAuditInfo(tag.auditInfo())
              .build();
        });

    TagEntity updatedTag = tagMetaService.getTagByIdentifier(tagEntity.nameIdentifier());
    Assertions.assertEquals("updated tag comment", updatedTag.comment());
  }

  @TestTemplate
  public void testCreateAndListTags() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity2, false);

    List<TagEntity> tagEntities =
        tagMetaService.listTagsByNamespace(NamespaceUtil.ofTag(METALAKE_NAME));
    Assertions.assertEquals(2, tagEntities.size());
    Assertions.assertTrue(tagEntities.contains(tagEntity1));
    Assertions.assertTrue(tagEntities.contains(tagEntity2));
  }

  @TestTemplate
  public void testUpdateTag() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    // Update with no tag entity.
    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                tagMetaService.updateTag(
                    NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"), tagEntity -> tagEntity));
    Assertions.assertEquals("No such tag entity: tag2", excep.getMessage());

    // Update tag entity.
    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(tagEntity1.id())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment1")
            .withProperties(ImmutableMap.of("k2", "v2"))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagEntity updatedTagEntity =
        tagMetaService.updateTag(
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1"), tagEntity -> tagEntity2);
    Assertions.assertEquals(tagEntity2, updatedTagEntity);

    TagEntity loadedTagEntity =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1"));
    Assertions.assertEquals(tagEntity2, loadedTagEntity);

    // Update with different id.
    TagEntity tagEntity3 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment1")
            .withProperties(ImmutableMap.of("k2", "v2"))
            .withAuditInfo(AUDIT_INFO)
            .build();

    Exception excep1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tagMetaService.updateTag(
                    NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1"), tagEntity -> tagEntity3));
    Assertions.assertEquals(
        "The updated tag entity id: "
            + tagEntity3.id()
            + " must have the same id as the old "
            + "entity id "
            + tagEntity2.id(),
        excep1.getMessage());

    TagEntity loadedTagEntity1 =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1"));
    Assertions.assertEquals(tagEntity2, loadedTagEntity1);
  }

  @TestTemplate
  public void testTagAlterDeleteAndOverwriteUseMonotonicVersion() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag_occ")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("initial")
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tag, false);

    TagPO initialPO =
        SessionUtils.getWithoutCommit(
            TagMetaMapper.class, mapper -> mapper.selectTagByTagId(tag.id()));
    tagMetaService.insertTag(tag, true);
    TagPO overwrittenPO =
        SessionUtils.getWithoutCommit(
            TagMetaMapper.class, mapper -> mapper.selectTagByTagId(tag.id()));
    Assertions.assertEquals(
        initialPO.getCurrentVersion() + 1, overwrittenPO.getCurrentVersion().longValue());
    Assertions.assertEquals(overwrittenPO.getCurrentVersion(), overwrittenPO.getLastVersion());

    TagEntity updatedTag = copyTagWithComment(tag, "updated");
    TagPO nextPO = POConverters.updateTagPOWithVersion(overwrittenPO, updatedTag);
    Assertions.assertEquals(
        Integer.valueOf(1),
        SessionUtils.doWithCommitAndFetchResult(
            TagMetaMapper.class, mapper -> mapper.updateTagMeta(nextPO, overwrittenPO)));
    Assertions.assertEquals(
        Integer.valueOf(0),
        SessionUtils.doWithCommitAndFetchResult(
            TagMetaMapper.class, mapper -> mapper.updateTagMeta(nextPO, overwrittenPO)));
    Assertions.assertEquals(
        Integer.valueOf(0),
        SessionUtils.doWithCommitAndFetchResult(
            TagMetaMapper.class,
            mapper ->
                mapper.softDeleteTagMetaByIdAndVersion(
                    tag.id(), overwrittenPO.getCurrentVersion())));
    Assertions.assertEquals(
        Integer.valueOf(1),
        SessionUtils.doWithCommitAndFetchResult(
            TagMetaMapper.class,
            mapper ->
                mapper.softDeleteTagMetaByIdAndVersion(tag.id(), nextPO.getCurrentVersion())));
  }

  @TestTemplate
  public void testTagAlterReportsOptimisticLockConflict() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag_alter_conflict")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("initial")
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tag, false);

    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            tagMetaService.updateTag(
                tag.nameIdentifier(),
                entity -> {
                  TagEntity current = (TagEntity) entity;
                  TagPO currentPO =
                      SessionUtils.getWithoutCommit(
                          TagMetaMapper.class, mapper -> mapper.selectTagByTagId(current.id()));
                  TagPO competingPO =
                      POConverters.updateTagPOWithVersion(
                          currentPO, copyTagWithComment(current, "competing"));
                  SessionUtils.doWithCommitAndFetchResult(
                      TagMetaMapper.class, mapper -> mapper.updateTagMeta(competingPO, currentPO));
                  return copyTagWithComment(current, "requested");
                }));
  }

  @TestTemplate
  public void testStaleTagDeleteRollsBackRelationshipCleanup() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, "catalog_tag_delete_occ");
    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag_delete_occ")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("initial")
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tag, false);
    tagMetaService.associateTagsWithMetadataObject(
        catalog.nameIdentifier(),
        catalog.type(),
        new NameIdentifier[] {tag.nameIdentifier()},
        new NameIdentifier[0]);
    TagPO stalePO =
        SessionUtils.getWithoutCommit(
            TagMetaMapper.class, mapper -> mapper.selectTagByTagId(tag.id()));
    tagMetaService.updateTag(
        tag.nameIdentifier(), entity -> copyTagWithComment((TagEntity) entity, "updated"));

    Assertions.assertThrows(
        OptimisticLockException.class,
        () -> tagMetaService.deleteTag(tag.nameIdentifier(), stalePO));
    Assertions.assertEquals(1, countActiveTagRel(tag.id()));
    Assertions.assertTrue(backend.exists(tag.nameIdentifier(), Entity.EntityType.TAG));

    Assertions.assertTrue(tagMetaService.deleteTag(tag.nameIdentifier()));
    Assertions.assertEquals(0, countActiveTagRel(tag.id()));
  }

  @TestTemplate
  public void testTagCreateIsFencedByParentMetalake() {
    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag_without_metalake")
            .withNamespace(NamespaceUtil.ofTag("metalake_that_does_not_exist"))
            .withComment("initial")
            .withAuditInfo(AUDIT_INFO)
            .build();

    Assertions.assertThrows(
        NoSuchEntityException.class, () -> tagMetaService.insertTag(tag, false));
    Assertions.assertThrows(NoSuchEntityException.class, () -> tagMetaService.insertTag(tag, true));
  }

  @TestTemplate
  public void testTagOverwriteAndAlterKeepAllowedValues() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag_allowed_values_occ")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("initial")
            .withAllowedValues(new String[] {"dev", "prod"})
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tag, false);

    tagMetaService.insertTag(copyTagWithComment(tag, "overwritten"), true);
    TagEntity overwritten = tagMetaService.getTagByIdentifier(tag.nameIdentifier());
    Assertions.assertArrayEquals(
        new String[] {"dev", "prod"}, overwritten.valueConstraint().allowedValues());

    tagMetaService.updateTag(
        tag.nameIdentifier(), entity -> copyTagWithComment((TagEntity) entity, "updated"));
    TagEntity updated = tagMetaService.getTagByIdentifier(tag.nameIdentifier());
    Assertions.assertEquals("updated", updated.comment());
    Assertions.assertArrayEquals(
        new String[] {"dev", "prod"}, updated.valueConstraint().allowedValues());
  }

  @TestTemplate
  public void testDeleteTag() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    boolean deleted = tagMetaService.deleteTag(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1"));
    Assertions.assertTrue(deleted);

    deleted = tagMetaService.deleteTag(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1"));
    Assertions.assertFalse(deleted);

    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1")));
    Assertions.assertEquals("No such tag entity: tag1", excep.getMessage());
  }

  @TestTemplate
  public void testDeleteMetalake() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1")));

    // Test delete metalake with cascade.
    BaseMetalake metalake1 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME + "1", AUDIT_INFO);
    backend.insert(metalake1, false);

    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME + "1"))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();

    tagMetaService.insertTag(tagEntity2, false);
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake1.nameIdentifier(), true));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            tagMetaService.getTagByIdentifier(
                NameIdentifierUtil.ofTag(METALAKE_NAME + "1", "tag2")));
  }

  @TestTemplate
  public void testAssociateAndDisassociateTagsWithMetadataObject() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    String catalogName = "catalog1";
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, catalogName);
    String schemaName = "schema1";
    SchemaEntity schema = createAndInsertSchema(METALAKE_NAME, catalogName, schemaName);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "table1",
            AUDIT_INFO);
    backend.insert(table, false);

    // Create tags to associate
    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity2, false);

    TagEntity tagEntity3 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag3")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity3, false);

    // Test associate tags with metadata object
    NameIdentifier[] tagsToAdd =
        new NameIdentifier[] {
          NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1"),
          NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"),
          NameIdentifierUtil.ofTag(METALAKE_NAME, "tag3")
        };

    List<TagEntity> tagEntities =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), tagsToAdd, new NameIdentifier[0]);
    Assertions.assertEquals(3, tagEntities.size());
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities, tagEntity1));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities, tagEntity2));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities, tagEntity3));

    // Test disassociate tags with metadata object
    NameIdentifier[] tagsToRemove =
        new NameIdentifier[] {NameIdentifierUtil.ofTag(METALAKE_NAME, "tag1")};

    List<TagEntity> tagEntities1 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), new NameIdentifier[0], tagsToRemove);

    Assertions.assertEquals(2, tagEntities1.size());
    Assertions.assertFalse(containsValuelessTagAssignment(tagEntities1, tagEntity1));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities1, tagEntity2));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities1, tagEntity3));

    // Test no tags to associate and disassociate
    List<TagEntity> tagEntities2 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), new NameIdentifier[0], new NameIdentifier[0]);
    Assertions.assertEquals(2, tagEntities2.size());
    Assertions.assertFalse(containsValuelessTagAssignment(tagEntities2, tagEntity1));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities2, tagEntity2));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities2, tagEntity3));

    // Test associate and disassociate same tags with metadata object
    List<TagEntity> tagEntities3 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), tagsToRemove, tagsToRemove);

    Assertions.assertEquals(2, tagEntities3.size());
    Assertions.assertFalse(containsValuelessTagAssignment(tagEntities3, tagEntity1));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities3, tagEntity2));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities3, tagEntity3));

    // Test associate and disassociate in-existent tags with metadata object
    NameIdentifier[] tagsToAdd1 =
        new NameIdentifier[] {
          NameIdentifierUtil.ofTag(METALAKE_NAME, "tag4"),
          NameIdentifierUtil.ofTag(METALAKE_NAME, "tag5")
        };

    NameIdentifier[] tagsToRemove1 =
        new NameIdentifier[] {
          NameIdentifierUtil.ofTag(METALAKE_NAME, "tag6"),
          NameIdentifierUtil.ofTag(METALAKE_NAME, "tag7")
        };

    List<TagEntity> tagEntities4 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), tagsToAdd1, tagsToRemove1);

    Assertions.assertEquals(2, tagEntities4.size());
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities4, tagEntity2));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities4, tagEntity3));

    // Test associate already associated tags with metadata object
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            tagMetaService.associateTagsWithMetadataObject(
                catalog.nameIdentifier(), catalog.type(), tagsToAdd, new NameIdentifier[0]));

    // Test disassociate already disassociated tags with metadata object
    List<TagEntity> tagEntities5 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), new NameIdentifier[0], tagsToRemove);

    Assertions.assertEquals(2, tagEntities5.size());
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities5, tagEntity2));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities5, tagEntity3));

    // Test associate and disassociate with invalid metadata object
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            tagMetaService.associateTagsWithMetadataObject(
                NameIdentifier.of(METALAKE_NAME, "non-existent-catalog"),
                catalog.type(),
                tagsToAdd,
                tagsToRemove));

    // Test associate and disassociate to a schema
    List<TagEntity> tagEntities6 =
        tagMetaService.associateTagsWithMetadataObject(
            schema.nameIdentifier(), schema.type(), tagsToAdd, tagsToRemove);

    Assertions.assertEquals(2, tagEntities6.size());
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities6, tagEntity2));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities6, tagEntity3));

    // Test associate and disassociate to a table
    List<TagEntity> tagEntities7 =
        tagMetaService.associateTagsWithMetadataObject(
            table.nameIdentifier(), table.type(), tagsToAdd, tagsToRemove);

    Assertions.assertEquals(2, tagEntities7.size());
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities7, tagEntity2));
    Assertions.assertTrue(containsValuelessTagAssignment(tagEntities7, tagEntity3));
  }

  @TestTemplate
  public void testAssociateTagValuesWithMetadataObject() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, "catalog_value");

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("stage")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("stage comment")
            .withProperties(props)
            .withAllowedValues(new String[] {"dev", "prod"})
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity, false);

    TagEntity storedTag =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(METALAKE_NAME, "stage"));
    Assertions.assertArrayEquals(
        new String[] {"dev", "prod"}, storedTag.valueConstraint().allowedValues());
    Assertions.assertFalse(storedTag.assignment().isPresent());

    List<TagEntity> tagEntities =
        backend.updateEntityRelations(
            RelationUpdate.of(
                SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                catalog.nameIdentifier(),
                catalog.type(),
                new RelationEdgeTarget[] {
                  RelationEdgeTarget.of(
                      NameIdentifierUtil.ofTag(METALAKE_NAME, "stage"),
                      Entity.EntityType.TAG,
                      "dev"),
                  RelationEdgeTarget.of(
                      NameIdentifierUtil.ofTag(METALAKE_NAME, "stage"),
                      Entity.EntityType.TAG,
                      "prod")
                },
                new RelationEdgeTarget[0]));

    Assertions.assertEquals(1, tagEntities.size());
    Assertions.assertEquals("stage", tagEntities.get(0).name());
    assertAssignmentValues(tagEntities.get(0), "dev", "prod");
    Assertions.assertEquals(2, countActiveTagRel(tagEntity.id()));

    TagEntity tagForMetadataObject =
        tagMetaService.getTagForMetadataObject(
            catalog.nameIdentifier(),
            catalog.type(),
            NameIdentifierUtil.ofTag(METALAKE_NAME, "stage"));
    assertAssignmentValues(tagForMetadataObject, "dev", "prod");

    List<GenericEntity> devMetadataObjects =
        backend.listEntitiesByRelation(
            RelationQuery.of(
                SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                NameIdentifierUtil.ofTag(METALAKE_NAME, "stage"),
                Entity.EntityType.TAG,
                true,
                "dev"));
    Assertions.assertEquals(1, devMetadataObjects.size());
    Assertions.assertTrue(
        containsGenericEntity(devMetadataObjects, "catalog_value", Entity.EntityType.CATALOG));

    List<GenericEntity> missingMetadataObjects =
        backend.listEntitiesByRelation(
            RelationQuery.of(
                SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                NameIdentifierUtil.ofTag(METALAKE_NAME, "stage"),
                Entity.EntityType.TAG,
                true,
                "missing"));
    Assertions.assertEquals(0, missingMetadataObjects.size());

    List<TagEntity> tagEntitiesAfterRemove =
        tagMetaService.associateTagValuesWithMetadataObject(
            catalog.nameIdentifier(),
            catalog.type(),
            new TagValue[0],
            new TagValue[] {TagValue.of("stage", "dev")});
    Assertions.assertEquals(1, tagEntitiesAfterRemove.size());
    assertAssignmentValues(tagEntitiesAfterRemove.get(0), "prod");
    Assertions.assertEquals(1, countActiveTagRel(tagEntity.id()));

    List<TagEntity> tagEntitiesAfterDuplicateAdd =
        tagMetaService.associateTagValuesWithMetadataObject(
            catalog.nameIdentifier(),
            catalog.type(),
            new TagValue[] {TagValue.of("stage", "prod")},
            new TagValue[0]);
    Assertions.assertEquals(1, tagEntitiesAfterDuplicateAdd.size());
    assertAssignmentValues(tagEntitiesAfterDuplicateAdd.get(0), "prod");
    Assertions.assertEquals(1, countActiveTagRel(tagEntity.id()));

    List<TagEntity> tagEntitiesAfterSameValueAddRemove =
        tagMetaService.associateTagValuesWithMetadataObject(
            catalog.nameIdentifier(),
            catalog.type(),
            new TagValue[] {TagValue.of("stage", "prod")},
            new TagValue[] {TagValue.of("stage", "prod")});
    Assertions.assertEquals(1, tagEntitiesAfterSameValueAddRemove.size());
    assertAssignmentValues(tagEntitiesAfterSameValueAddRemove.get(0), "prod");
    Assertions.assertEquals(1, countActiveTagRel(tagEntity.id()));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tagMetaService.associateTagValuesWithMetadataObject(
                    catalog.nameIdentifier(),
                    catalog.type(),
                    new TagValue[] {TagValue.of("stage", "qa")},
                    new TagValue[0]));
    Assertions.assertTrue(exception.getMessage().contains("is not in allowed values"));

    IllegalArgumentException noValueException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tagMetaService.associateTagValuesWithMetadataObject(
                    catalog.nameIdentifier(),
                    catalog.type(),
                    new TagValue[] {TagValue.noValue("stage")},
                    new TagValue[0]));
    Assertions.assertTrue(noValueException.getMessage().contains("requires assignment values"));
  }

  @TestTemplate
  public void testRejectDuplicateValuelessTagAssignment() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    CatalogEntity catalog = createAndInsertCatalog(METALAKE_NAME, "catalog_unique_value");

    TagEntity tagEntity =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("unique_value")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("unique value comment")
            .withAuditInfo(AUDIT_INFO)
            .build();
    TagMetaService tagMetaService = TagMetaService.getInstance();
    tagMetaService.insertTag(tagEntity, false);
    tagMetaService.associateTagValuesWithMetadataObject(
        catalog.nameIdentifier(),
        catalog.type(),
        new TagValue[] {TagValue.noValue(tagEntity.name())},
        new TagValue[0]);

    Assertions.assertThrows(SQLException.class, () -> insertDuplicateActiveTagRel(tagEntity.id()));
  }

  @TestTemplate
  public void testListTagsForMetadataObject() throws IOException {
    testAssociateAndDisassociateTagsWithMetadataObject();

    TagMetaService tagMetaService = TagMetaService.getInstance();

    // Test list tags for catalog
    List<TagEntity> tagEntities =
        tagMetaService.listTagsForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1"), Entity.EntityType.CATALOG);
    Assertions.assertEquals(2, tagEntities.size());
    Assertions.assertTrue(
        tagEntities.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag2")));
    Assertions.assertTrue(
        tagEntities.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag3")));

    // Test list tags for schema
    List<TagEntity> tagEntities1 =
        tagMetaService.listTagsForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1"), Entity.EntityType.SCHEMA);

    Assertions.assertEquals(2, tagEntities1.size());
    Assertions.assertTrue(
        tagEntities1.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag2")));
    Assertions.assertTrue(
        tagEntities1.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag3")));

    // Test list tags for table
    List<TagEntity> tagEntities2 =
        tagMetaService.listTagsForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table1"),
            Entity.EntityType.TABLE);

    Assertions.assertEquals(2, tagEntities2.size());
    Assertions.assertTrue(
        tagEntities2.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag2")));
    Assertions.assertTrue(
        tagEntities2.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag3")));

    // Test list tags for non-existent metadata object
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            tagMetaService.listTagsForMetadataObject(
                NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table2"),
                Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testGetTagForMetadataObject() throws IOException {
    testAssociateAndDisassociateTagsWithMetadataObject();

    TagMetaService tagMetaService = TagMetaService.getInstance();

    // Test get tag for catalog
    TagEntity tagEntity =
        tagMetaService.getTagForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1"),
            Entity.EntityType.CATALOG,
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"));
    Assertions.assertEquals("tag2", tagEntity.name());

    // Test get tag for schema
    TagEntity tagEntity1 =
        tagMetaService.getTagForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1"),
            Entity.EntityType.SCHEMA,
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag3"));
    Assertions.assertEquals("tag3", tagEntity1.name());

    // Test get tag for table
    TagEntity tagEntity2 =
        tagMetaService.getTagForMetadataObject(
            NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table1"),
            Entity.EntityType.TABLE,
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"));
    Assertions.assertEquals("tag2", tagEntity2.name());

    // Test get tag for non-existent metadata object
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            tagMetaService.getTagForMetadataObject(
                NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table2"),
                Entity.EntityType.TABLE,
                NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2")));

    // Test get tag for non-existent tag
    Throwable e =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                tagMetaService.getTagForMetadataObject(
                    NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table1"),
                    Entity.EntityType.TABLE,
                    NameIdentifierUtil.ofTag(METALAKE_NAME, "tag4")));
    Assertions.assertTrue(e.getMessage().contains("No such tag entity: tag4"));
  }

  @TestTemplate
  public void testListAssociatedMetadataObjectsForTag() throws IOException {
    testAssociateAndDisassociateTagsWithMetadataObject();

    TagMetaService tagMetaService = TagMetaService.getInstance();

    // Test list associated metadata objects for tag2
    List<GenericEntity> metadataObjects =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"));

    Assertions.assertEquals(3, metadataObjects.size());
    Assertions.assertTrue(
        containsGenericEntity(metadataObjects, "catalog1", Entity.EntityType.CATALOG));
    Assertions.assertTrue(
        containsGenericEntity(metadataObjects, "catalog1.schema1", Entity.EntityType.SCHEMA));
    Assertions.assertTrue(
        containsGenericEntity(metadataObjects, "catalog1.schema1.table1", Entity.EntityType.TABLE));

    // Test list associated metadata objects for tag3
    List<GenericEntity> metadataObjects1 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag3"));

    Assertions.assertEquals(3, metadataObjects1.size());

    Assertions.assertTrue(
        containsGenericEntity(metadataObjects1, "catalog1", Entity.EntityType.CATALOG));
    Assertions.assertTrue(
        containsGenericEntity(metadataObjects1, "catalog1.schema1", Entity.EntityType.SCHEMA));
    Assertions.assertTrue(
        containsGenericEntity(
            metadataObjects1, "catalog1.schema1.table1", Entity.EntityType.TABLE));

    // Test list associated metadata objects for non-existent tag
    List<GenericEntity> metadataObjects2 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag4"));
    Assertions.assertEquals(0, metadataObjects2.size());

    // Test metadata object non-exist scenario.
    backend.delete(
        NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1", "table1"),
        Entity.EntityType.TABLE,
        false);

    List<GenericEntity> metadataObjects3 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"));

    Assertions.assertEquals(2, metadataObjects3.size());

    Assertions.assertTrue(
        containsGenericEntity(metadataObjects3, "catalog1", Entity.EntityType.CATALOG));
    Assertions.assertTrue(
        containsGenericEntity(metadataObjects3, "catalog1.schema1", Entity.EntityType.SCHEMA));

    backend.delete(
        NameIdentifier.of(METALAKE_NAME, "catalog1", "schema1"), Entity.EntityType.SCHEMA, false);

    List<GenericEntity> metadataObjects4 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"));

    Assertions.assertEquals(1, metadataObjects4.size());
    Assertions.assertTrue(
        containsGenericEntity(metadataObjects4, "catalog1", Entity.EntityType.CATALOG));

    backend.delete(NameIdentifier.of(METALAKE_NAME, "catalog1"), Entity.EntityType.CATALOG, false);

    List<GenericEntity> metadataObjects5 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(METALAKE_NAME, "tag2"));

    Assertions.assertEquals(0, metadataObjects5.size());
  }

  @TestTemplate
  public void testDeleteMetadataObjectForTag() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, AUDIT_INFO);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME),
            "catalog1",
            AUDIT_INFO);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name()),
            "schema1",
            AUDIT_INFO);
    backend.insert(schema, false);

    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(AUDIT_INFO)
            .build();

    List<ColumnEntity> columns = Lists.newArrayList();
    columns.add(column);

    TableEntity table =
        TableEntity.builder()
            .withName("table")
            .withNamespace(Namespace.of(METALAKE_NAME, catalog.name(), schema.name()))
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withColumns(columns)
            .withAuditInfo(AUDIT_INFO)
            .build();

    backend.insert(table, false);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "topic1",
            AUDIT_INFO);
    backend.insert(topic, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "fileset1",
            AUDIT_INFO);
    backend.insert(fileset, false);

    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "model1",
            "comment",
            1,
            null,
            AUDIT_INFO);
    backend.insert(model, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(METALAKE_NAME))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(AUDIT_INFO)
            .build();
    tagMetaService.insertTag(tagEntity1, false);
    tagMetaService.associateTagsWithMetadataObject(
        catalog.nameIdentifier(),
        catalog.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        schema.nameIdentifier(),
        schema.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        table.nameIdentifier(),
        table.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        topic.nameIdentifier(),
        topic.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        fileset.nameIdentifier(),
        fileset.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    NameIdentifier columnIdentifier =
        NameIdentifier.of(Namespace.fromString(table.nameIdentifier().toString()), column.name());
    tagMetaService.associateTagsWithMetadataObject(
        columnIdentifier,
        column.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        model.nameIdentifier(),
        model.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);

    Assertions.assertEquals(7, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(7, countAllTagRel(tagEntity1.id()));

    // Test to delete a model
    ModelMetaService.getInstance().deleteModel(model.nameIdentifier());
    Assertions.assertEquals(6, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(7, countAllTagRel(tagEntity1.id()));

    // Test to drop a table
    TableMetaService.getInstance().deleteTable(table.nameIdentifier());
    Assertions.assertEquals(4, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(7, countAllTagRel(tagEntity1.id()));

    // Test to drop a topic
    TopicMetaService.getInstance().deleteTopic(topic.nameIdentifier());
    Assertions.assertEquals(3, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(7, countAllTagRel(tagEntity1.id()));

    // Test to drop a fileset
    FilesetMetaService.getInstance().deleteFileset(fileset.nameIdentifier());
    Assertions.assertEquals(2, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(7, countAllTagRel(tagEntity1.id()));

    // Test to drop a schema
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), false);
    Assertions.assertEquals(1, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(7, countAllTagRel(tagEntity1.id()));

    // Test to drop a catalog
    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), false);
    Assertions.assertEquals(0, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(7, countAllTagRel(tagEntity1.id()));

    // Test to drop a catalog using cascade mode
    catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME),
            "catalog1",
            AUDIT_INFO);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name()),
            "schema1",
            AUDIT_INFO);
    backend.insert(schema, false);

    column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(AUDIT_INFO)
            .build();

    columns = Lists.newArrayList();
    columns.add(column);

    table =
        TableEntity.builder()
            .withName("table")
            .withNamespace(Namespace.of(METALAKE_NAME, catalog.name(), schema.name()))
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withColumns(columns)
            .withAuditInfo(AUDIT_INFO)
            .build();

    backend.insert(table, false);

    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "topic1",
            AUDIT_INFO);
    backend.insert(topic, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "fileset1",
            AUDIT_INFO);
    backend.insert(fileset, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "model1",
            "comment",
            1,
            null,
            AUDIT_INFO);
    backend.insert(model, false);

    tagMetaService.associateTagsWithMetadataObject(
        catalog.nameIdentifier(),
        catalog.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        schema.nameIdentifier(),
        schema.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        table.nameIdentifier(),
        table.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        topic.nameIdentifier(),
        topic.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        fileset.nameIdentifier(),
        fileset.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    columnIdentifier =
        NameIdentifier.of(Namespace.fromString(table.nameIdentifier().toString()), column.name());
    tagMetaService.associateTagsWithMetadataObject(
        columnIdentifier,
        column.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        model.nameIdentifier(),
        model.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);

    CatalogMetaService.getInstance().deleteCatalog(catalog.nameIdentifier(), true);
    Assertions.assertEquals(0, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(14, countAllTagRel(tagEntity1.id()));

    // Test to drop a schema using cascade mode
    catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME),
            "catalog1",
            AUDIT_INFO);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name()),
            "schema1",
            AUDIT_INFO);
    backend.insert(schema, false);

    column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(AUDIT_INFO)
            .build();

    columns = Lists.newArrayList();
    columns.add(column);

    table =
        TableEntity.builder()
            .withName("table")
            .withNamespace(Namespace.of(METALAKE_NAME, catalog.name(), schema.name()))
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withColumns(columns)
            .withAuditInfo(AUDIT_INFO)
            .build();

    backend.insert(table, false);

    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "topic1",
            AUDIT_INFO);
    backend.insert(topic, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "fileset1",
            AUDIT_INFO);
    backend.insert(fileset, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalog.name(), schema.name()),
            "model1",
            "comment",
            1,
            null,
            AUDIT_INFO);
    backend.insert(model, false);

    tagMetaService.associateTagsWithMetadataObject(
        catalog.nameIdentifier(),
        catalog.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        schema.nameIdentifier(),
        schema.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        table.nameIdentifier(),
        table.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        topic.nameIdentifier(),
        topic.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        fileset.nameIdentifier(),
        fileset.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    columnIdentifier =
        NameIdentifier.of(Namespace.fromString(table.nameIdentifier().toString()), column.name());
    tagMetaService.associateTagsWithMetadataObject(
        columnIdentifier,
        column.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);
    tagMetaService.associateTagsWithMetadataObject(
        model.nameIdentifier(),
        model.type(),
        new NameIdentifier[] {tagEntity1.nameIdentifier()},
        new NameIdentifier[0]);

    // Test to drop a schema
    SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true);
    Assertions.assertEquals(1, countActiveTagRel(tagEntity1.id()));
    Assertions.assertEquals(21, countAllTagRel(tagEntity1.id()));
  }

  @TestTemplate
  public void testGetTagIdByTagNameWhenTagNotFound() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(METALAKE_NAME);

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> tagMetaService.getTagIdByTagName(metalakeId, "missing_tag"));
  }

  private TagEntity copyTagWithComment(TagEntity tag, String comment) {
    TagEntity.Builder builder =
        TagEntity.builder()
            .withId(tag.id())
            .withName(tag.name())
            .withNamespace(tag.namespace())
            .withComment(comment)
            .withProperties(tag.properties())
            .withAuditInfo(tag.auditInfo());
    if (tag.valueConstraint().type() != TagValueConstraint.Type.ANY_VALUE) {
      builder.withAllowedValues(tag.valueConstraint().allowedValues());
    }
    return builder.build();
  }

  private boolean containsValuelessTagAssignment(
      List<TagEntity> tagEntities, TagEntity expectedTagEntity) {
    return tagEntities.stream()
        .anyMatch(
            tagEntity ->
                tagEntity.id().equals(expectedTagEntity.id())
                    && tagEntity.name().equals(expectedTagEntity.name())
                    && tagEntity.assignment().isPresent()
                    && !tagEntity.assignment().get().hasValues());
  }

  private void assertAssignmentValues(TagEntity tagEntity, String... expectedValues) {
    assertTrue(tagEntity.assignment().isPresent());
    assertEquals(
        new LinkedHashSet<>(Arrays.asList(expectedValues)),
        new LinkedHashSet<>(Arrays.asList(tagEntity.assignment().get().values())));
  }

  private boolean containsGenericEntity(
      List<GenericEntity> genericEntities, String name, Entity.EntityType entityType) {
    return genericEntities.stream().anyMatch(e -> e.name().equals(name) && e.type() == entityType);
  }

  private Integer countAllTagRel(Long tagId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format("SELECT count(*) FROM tag_relation_meta WHERE tag_id = %d", tagId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }

  private void insertDuplicateActiveTagRel(Long tagId) throws SQLException {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          String.format(
              "INSERT INTO tag_relation_meta (tag_id, metadata_object_id, metadata_object_type, tag_value, audit_info, current_version, last_version, deleted_at) "
                  + "SELECT tag_id, metadata_object_id, metadata_object_type, tag_value, audit_info, current_version, last_version, deleted_at "
                  + "FROM tag_relation_meta WHERE tag_id = %d AND deleted_at = 0",
              tagId));
    }
  }

  private Integer countActiveTagRel(Long tagId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement1 = connection.createStatement();
        ResultSet rs1 =
            statement1.executeQuery(
                String.format(
                    "SELECT count(*) FROM tag_relation_meta WHERE tag_id = %d AND deleted_at = 0",
                    tagId))) {
      if (rs1.next()) {
        return rs1.getInt(1);
      } else {
        throw new RuntimeException("Doesn't contain data");
      }
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }
}
