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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagMetaService extends TestJDBCBackend {

  private final String metalakeName = "metalake_for_tag_test";

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  private final Map<String, String> props = ImmutableMap.of("k1", "v1");

  @Test
  public void testInsertAndGetTagByIdentifier() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    // Test no tag entity.
    TagMetaService tagMetaService = TagMetaService.getInstance();
    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(metalakeName, "tag1")));
    Assertions.assertEquals("No such tag entity: tag1", excep.getMessage());

    // Test get tag entity
    TagEntity tagEntity =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity, false);

    TagEntity resultTagEntity =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(metalakeName, "tag1"));
    Assertions.assertEquals(tagEntity, resultTagEntity);

    // Test with null comment and properties.
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withAuditInfo(auditInfo)
            .build();

    tagMetaService.insertTag(tagEntity1, false);
    TagEntity resultTagEntity1 =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(metalakeName, "tag2"));
    Assertions.assertEquals(tagEntity1, resultTagEntity1);
    Assertions.assertNull(resultTagEntity1.comment());
    Assertions.assertNull(resultTagEntity1.properties());

    // Test insert with overwrite.
    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(tagEntity1.id())
            .withName("tag3")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertThrows(Exception.class, () -> tagMetaService.insertTag(tagEntity2, false));

    tagMetaService.insertTag(tagEntity2, true);

    TagEntity resultTagEntity2 =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(metalakeName, "tag3"));
    Assertions.assertEquals(tagEntity2, resultTagEntity2);
  }

  @Test
  public void testCreateAndListTags() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity2, false);

    List<TagEntity> tagEntities =
        tagMetaService.listTagsByNamespace(NamespaceUtil.ofTag(metalakeName));
    Assertions.assertEquals(2, tagEntities.size());
    Assertions.assertTrue(tagEntities.contains(tagEntity1));
    Assertions.assertTrue(tagEntities.contains(tagEntity2));
  }

  @Test
  public void testUpdateTag() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    // Update with no tag entity.
    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                tagMetaService.updateTag(
                    NameIdentifierUtil.ofTag(metalakeName, "tag2"), tagEntity -> tagEntity));
    Assertions.assertEquals("No such tag entity: tag2", excep.getMessage());

    // Update tag entity.
    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(tagEntity1.id())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment1")
            .withProperties(ImmutableMap.of("k2", "v2"))
            .withAuditInfo(auditInfo)
            .build();
    TagEntity updatedTagEntity =
        tagMetaService.updateTag(
            NameIdentifierUtil.ofTag(metalakeName, "tag1"), tagEntity -> tagEntity2);
    Assertions.assertEquals(tagEntity2, updatedTagEntity);

    TagEntity loadedTagEntity =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(metalakeName, "tag1"));
    Assertions.assertEquals(tagEntity2, loadedTagEntity);

    // Update with different id.
    TagEntity tagEntity3 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment1")
            .withProperties(ImmutableMap.of("k2", "v2"))
            .withAuditInfo(auditInfo)
            .build();

    Exception excep1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tagMetaService.updateTag(
                    NameIdentifierUtil.ofTag(metalakeName, "tag1"), tagEntity -> tagEntity3));
    Assertions.assertEquals(
        "The updated tag entity id: "
            + tagEntity3.id()
            + " must have the same id as the old "
            + "entity id "
            + tagEntity2.id(),
        excep1.getMessage());

    TagEntity loadedTagEntity1 =
        tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(metalakeName, "tag1"));
    Assertions.assertEquals(tagEntity2, loadedTagEntity1);
  }

  @Test
  public void testDeleteTag() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    boolean deleted = tagMetaService.deleteTag(NameIdentifierUtil.ofTag(metalakeName, "tag1"));
    Assertions.assertTrue(deleted);

    deleted = tagMetaService.deleteTag(NameIdentifierUtil.ofTag(metalakeName, "tag1"));
    Assertions.assertFalse(deleted);

    Exception excep =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(metalakeName, "tag1")));
    Assertions.assertEquals("No such tag entity: tag1", excep.getMessage());
  }

  @Test
  public void testDeleteMetalake() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> tagMetaService.getTagByIdentifier(NameIdentifierUtil.ofTag(metalakeName, "tag1")));

    // Test delete metalake with cascade.
    BaseMetalake metalake1 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName + "1", auditInfo);
    backend.insert(metalake1, false);

    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(NamespaceUtil.ofTag(metalakeName + "1"))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();

    tagMetaService.insertTag(tagEntity2, false);
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake1.nameIdentifier(), true));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            tagMetaService.getTagByIdentifier(
                NameIdentifierUtil.ofTag(metalakeName + "1", "tag2")));
  }

  @Test
  public void testAssociateAndDisassociateTagsWithMetadataObject() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of(metalakeName), "catalog1", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name()),
            "schema1",
            auditInfo);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "table1",
            auditInfo);
    backend.insert(table, false);

    // Create tags to associate
    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity2, false);

    TagEntity tagEntity3 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag3")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity3, false);

    // Test associate tags with metadata object
    NameIdentifier[] tagsToAdd =
        new NameIdentifier[] {
          NameIdentifierUtil.ofTag(metalakeName, "tag1"),
          NameIdentifierUtil.ofTag(metalakeName, "tag2"),
          NameIdentifierUtil.ofTag(metalakeName, "tag3")
        };

    List<TagEntity> tagEntities =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), tagsToAdd, new NameIdentifier[0]);
    Assertions.assertEquals(3, tagEntities.size());
    Assertions.assertTrue(tagEntities.contains(tagEntity1));
    Assertions.assertTrue(tagEntities.contains(tagEntity2));
    Assertions.assertTrue(tagEntities.contains(tagEntity3));

    // Test disassociate tags with metadata object
    NameIdentifier[] tagsToRemove =
        new NameIdentifier[] {NameIdentifierUtil.ofTag(metalakeName, "tag1")};

    List<TagEntity> tagEntities1 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), new NameIdentifier[0], tagsToRemove);

    Assertions.assertEquals(2, tagEntities1.size());
    Assertions.assertFalse(tagEntities1.contains(tagEntity1));
    Assertions.assertTrue(tagEntities1.contains(tagEntity2));
    Assertions.assertTrue(tagEntities1.contains(tagEntity3));

    // Test no tags to associate and disassociate
    List<TagEntity> tagEntities2 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), new NameIdentifier[0], new NameIdentifier[0]);
    Assertions.assertEquals(2, tagEntities2.size());
    Assertions.assertFalse(tagEntities2.contains(tagEntity1));
    Assertions.assertTrue(tagEntities2.contains(tagEntity2));
    Assertions.assertTrue(tagEntities2.contains(tagEntity3));

    // Test associate and disassociate same tags with metadata object
    List<TagEntity> tagEntities3 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), tagsToRemove, tagsToRemove);

    Assertions.assertEquals(2, tagEntities3.size());
    Assertions.assertFalse(tagEntities3.contains(tagEntity1));
    Assertions.assertTrue(tagEntities3.contains(tagEntity2));
    Assertions.assertTrue(tagEntities3.contains(tagEntity3));

    // Test associate and disassociate in-existent tags with metadata object
    NameIdentifier[] tagsToAdd1 =
        new NameIdentifier[] {
          NameIdentifierUtil.ofTag(metalakeName, "tag4"),
          NameIdentifierUtil.ofTag(metalakeName, "tag5")
        };

    NameIdentifier[] tagsToRemove1 =
        new NameIdentifier[] {
          NameIdentifierUtil.ofTag(metalakeName, "tag6"),
          NameIdentifierUtil.ofTag(metalakeName, "tag7")
        };

    List<TagEntity> tagEntities4 =
        tagMetaService.associateTagsWithMetadataObject(
            catalog.nameIdentifier(), catalog.type(), tagsToAdd1, tagsToRemove1);

    Assertions.assertEquals(2, tagEntities4.size());
    Assertions.assertTrue(tagEntities4.contains(tagEntity2));
    Assertions.assertTrue(tagEntities4.contains(tagEntity3));

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
    Assertions.assertTrue(tagEntities5.contains(tagEntity2));
    Assertions.assertTrue(tagEntities5.contains(tagEntity3));

    // Test associate and disassociate with invalid metadata object
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            tagMetaService.associateTagsWithMetadataObject(
                NameIdentifier.of(metalakeName, "non-existent-catalog"),
                catalog.type(),
                tagsToAdd,
                tagsToRemove));

    // Test associate and disassociate to a schema
    List<TagEntity> tagEntities6 =
        tagMetaService.associateTagsWithMetadataObject(
            schema.nameIdentifier(), schema.type(), tagsToAdd, tagsToRemove);

    Assertions.assertEquals(2, tagEntities6.size());
    Assertions.assertTrue(tagEntities6.contains(tagEntity2));
    Assertions.assertTrue(tagEntities6.contains(tagEntity3));

    // Test associate and disassociate to a table
    List<TagEntity> tagEntities7 =
        tagMetaService.associateTagsWithMetadataObject(
            table.nameIdentifier(), table.type(), tagsToAdd, tagsToRemove);

    Assertions.assertEquals(2, tagEntities7.size());
    Assertions.assertTrue(tagEntities7.contains(tagEntity2));
    Assertions.assertTrue(tagEntities7.contains(tagEntity3));
  }

  @Test
  public void testListTagsForMetadataObject() throws IOException {
    testAssociateAndDisassociateTagsWithMetadataObject();

    TagMetaService tagMetaService = TagMetaService.getInstance();

    // Test list tags for catalog
    List<TagEntity> tagEntities =
        tagMetaService.listTagsForMetadataObject(
            NameIdentifier.of(metalakeName, "catalog1"), Entity.EntityType.CATALOG);
    Assertions.assertEquals(2, tagEntities.size());
    Assertions.assertTrue(
        tagEntities.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag2")));
    Assertions.assertTrue(
        tagEntities.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag3")));

    // Test list tags for schema
    List<TagEntity> tagEntities1 =
        tagMetaService.listTagsForMetadataObject(
            NameIdentifier.of(metalakeName, "catalog1", "schema1"), Entity.EntityType.SCHEMA);

    Assertions.assertEquals(2, tagEntities1.size());
    Assertions.assertTrue(
        tagEntities1.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag2")));
    Assertions.assertTrue(
        tagEntities1.stream().anyMatch(tagEntity -> tagEntity.name().equals("tag3")));

    // Test list tags for table
    List<TagEntity> tagEntities2 =
        tagMetaService.listTagsForMetadataObject(
            NameIdentifier.of(metalakeName, "catalog1", "schema1", "table1"),
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
                NameIdentifier.of(metalakeName, "catalog1", "schema1", "table2"),
                Entity.EntityType.TABLE));
  }

  @Test
  public void testGetTagForMetadataObject() throws IOException {
    testAssociateAndDisassociateTagsWithMetadataObject();

    TagMetaService tagMetaService = TagMetaService.getInstance();

    // Test get tag for catalog
    TagEntity tagEntity =
        tagMetaService.getTagForMetadataObject(
            NameIdentifier.of(metalakeName, "catalog1"),
            Entity.EntityType.CATALOG,
            NameIdentifierUtil.ofTag(metalakeName, "tag2"));
    Assertions.assertEquals("tag2", tagEntity.name());

    // Test get tag for schema
    TagEntity tagEntity1 =
        tagMetaService.getTagForMetadataObject(
            NameIdentifier.of(metalakeName, "catalog1", "schema1"),
            Entity.EntityType.SCHEMA,
            NameIdentifierUtil.ofTag(metalakeName, "tag3"));
    Assertions.assertEquals("tag3", tagEntity1.name());

    // Test get tag for table
    TagEntity tagEntity2 =
        tagMetaService.getTagForMetadataObject(
            NameIdentifier.of(metalakeName, "catalog1", "schema1", "table1"),
            Entity.EntityType.TABLE,
            NameIdentifierUtil.ofTag(metalakeName, "tag2"));
    Assertions.assertEquals("tag2", tagEntity2.name());

    // Test get tag for non-existent metadata object
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            tagMetaService.getTagForMetadataObject(
                NameIdentifier.of(metalakeName, "catalog1", "schema1", "table2"),
                Entity.EntityType.TABLE,
                NameIdentifierUtil.ofTag(metalakeName, "tag2")));

    // Test get tag for non-existent tag
    Throwable e =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                tagMetaService.getTagForMetadataObject(
                    NameIdentifier.of(metalakeName, "catalog1", "schema1", "table1"),
                    Entity.EntityType.TABLE,
                    NameIdentifierUtil.ofTag(metalakeName, "tag4")));
    Assertions.assertTrue(e.getMessage().contains("No such tag entity: tag4"));
  }

  @Test
  public void testListAssociatedMetadataObjectsForTag() throws IOException {
    testAssociateAndDisassociateTagsWithMetadataObject();

    TagMetaService tagMetaService = TagMetaService.getInstance();

    // Test list associated metadata objects for tag2
    List<MetadataObject> metadataObjects =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(metalakeName, "tag2"));

    Assertions.assertEquals(3, metadataObjects.size());
    Assertions.assertTrue(
        metadataObjects.contains(MetadataObjects.parse("catalog1", MetadataObject.Type.CATALOG)));
    Assertions.assertTrue(
        metadataObjects.contains(
            MetadataObjects.parse("catalog1.schema1", MetadataObject.Type.SCHEMA)));
    Assertions.assertTrue(
        metadataObjects.contains(
            MetadataObjects.parse("catalog1.schema1.table1", MetadataObject.Type.TABLE)));

    // Test list associated metadata objects for tag3
    List<MetadataObject> metadataObjects1 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(metalakeName, "tag3"));

    Assertions.assertEquals(3, metadataObjects1.size());
    Assertions.assertTrue(
        metadataObjects1.contains(MetadataObjects.parse("catalog1", MetadataObject.Type.CATALOG)));
    Assertions.assertTrue(
        metadataObjects1.contains(
            MetadataObjects.parse("catalog1.schema1", MetadataObject.Type.SCHEMA)));
    Assertions.assertTrue(
        metadataObjects1.contains(
            MetadataObjects.parse("catalog1.schema1.table1", MetadataObject.Type.TABLE)));

    // Test list associated metadata objects for non-existent tag
    List<MetadataObject> metadataObjects2 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(metalakeName, "tag4"));
    Assertions.assertEquals(0, metadataObjects2.size());

    // Test metadata object non-exist scenario.
    backend.delete(
        NameIdentifier.of(metalakeName, "catalog1", "schema1", "table1"),
        Entity.EntityType.TABLE,
        false);

    List<MetadataObject> metadataObjects3 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(metalakeName, "tag2"));

    Assertions.assertEquals(2, metadataObjects3.size());
    Assertions.assertTrue(
        metadataObjects3.contains(MetadataObjects.parse("catalog1", MetadataObject.Type.CATALOG)));
    Assertions.assertTrue(
        metadataObjects3.contains(
            MetadataObjects.parse("catalog1.schema1", MetadataObject.Type.SCHEMA)));

    backend.delete(
        NameIdentifier.of(metalakeName, "catalog1", "schema1"), Entity.EntityType.SCHEMA, false);

    List<MetadataObject> metadataObjects4 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(metalakeName, "tag2"));

    Assertions.assertEquals(1, metadataObjects4.size());
    Assertions.assertTrue(
        metadataObjects4.contains(MetadataObjects.parse("catalog1", MetadataObject.Type.CATALOG)));

    backend.delete(NameIdentifier.of(metalakeName, "catalog1"), Entity.EntityType.CATALOG, false);

    List<MetadataObject> metadataObjects5 =
        tagMetaService.listAssociatedMetadataObjectsForTag(
            NameIdentifierUtil.ofTag(metalakeName, "tag2"));

    Assertions.assertEquals(0, metadataObjects5.size());
  }

  @Test
  public void testDeleteMetadataObjectForTag() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of(metalakeName), "catalog1", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name()),
            "schema1",
            auditInfo);
    backend.insert(schema, false);

    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(auditInfo)
            .build();

    List<ColumnEntity> columns = Lists.newArrayList();
    columns.add(column);

    TableEntity table =
        TableEntity.builder()
            .withName("table")
            .withNamespace(Namespace.of(metalakeName, catalog.name(), schema.name()))
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withColumns(columns)
            .withAuditInfo(auditInfo)
            .build();

    backend.insert(table, false);

    TopicEntity topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "topic1",
            auditInfo);
    backend.insert(topic, false);

    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "fileset1",
            auditInfo);
    backend.insert(fileset, false);

    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "model1",
            "comment",
            1,
            null,
            auditInfo);
    backend.insert(model, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
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
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of(metalakeName), "catalog1", auditInfo);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name()),
            "schema1",
            auditInfo);
    backend.insert(schema, false);

    column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(auditInfo)
            .build();

    columns = Lists.newArrayList();
    columns.add(column);

    table =
        TableEntity.builder()
            .withName("table")
            .withNamespace(Namespace.of(metalakeName, catalog.name(), schema.name()))
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withColumns(columns)
            .withAuditInfo(auditInfo)
            .build();

    backend.insert(table, false);

    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "topic1",
            auditInfo);
    backend.insert(topic, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "fileset1",
            auditInfo);
    backend.insert(fileset, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "model1",
            "comment",
            1,
            null,
            auditInfo);
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
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of(metalakeName), "catalog1", auditInfo);
    backend.insert(catalog, false);

    schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name()),
            "schema1",
            auditInfo);
    backend.insert(schema, false);

    column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withAutoIncrement(false)
            .withNullable(false)
            .withDataType(Types.IntegerType.get())
            .withAuditInfo(auditInfo)
            .build();

    columns = Lists.newArrayList();
    columns.add(column);

    table =
        TableEntity.builder()
            .withName("table")
            .withNamespace(Namespace.of(metalakeName, catalog.name(), schema.name()))
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withColumns(columns)
            .withAuditInfo(auditInfo)
            .build();

    backend.insert(table, false);

    topic =
        createTopicEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "topic1",
            auditInfo);
    backend.insert(topic, false);

    fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "fileset1",
            auditInfo);
    backend.insert(fileset, false);

    model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalog.name(), schema.name()),
            "model1",
            "comment",
            1,
            null,
            auditInfo);
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
}
