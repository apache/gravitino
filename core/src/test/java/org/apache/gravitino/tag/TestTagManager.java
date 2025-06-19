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
package org.apache.gravitino.tag;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestTagManager {

  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static final Config config = Mockito.mock(Config.class);

  private static final String METALAKE = "metalake_for_tag_test";

  private static final String CATALOG = "catalog_for_tag_test";

  private static final String SCHEMA = "schema_for_tag_test";

  private static final String TABLE = "table_for_tag_test";

  private static final String COLUMN = "column_for_tag_test";

  private static final MetalakeDispatcher metalakeDispatcher = mock(MetalakeDispatcher.class);
  private static final CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);
  private static final SchemaDispatcher schemaDispatcher = mock(SchemaDispatcher.class);
  private static final TableDispatcher tableDispatcher = mock(TableDispatcher.class);

  private static EntityStore entityStore;

  private static IdGenerator idGenerator;

  private static TagManager tagManager;

  @BeforeAll
  public static void setUp() throws IOException, IllegalAccessException {
    idGenerator = new RandomIdGenerator();

    File dbDir = new File(DB_DIR);
    dbDir.mkdirs();

    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    // Fix cache config for test
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");

    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(METALAKE)
            .withVersion(SchemaVersion.V_0_1)
            .withComment("Test metalake")
            .withAuditInfo(audit)
            .build();
    entityStore.put(metalake, false /* overwritten */);

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(idGenerator.nextId())
            .withName(CATALOG)
            .withNamespace(Namespace.of(METALAKE))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("Test catalog")
            .withAuditInfo(audit)
            .build();
    entityStore.put(catalog, false /* overwritten */);

    SchemaEntity schema =
        SchemaEntity.builder()
            .withId(idGenerator.nextId())
            .withName(SCHEMA)
            .withNamespace(Namespace.of(METALAKE, CATALOG))
            .withComment("Test schema")
            .withAuditInfo(audit)
            .build();
    entityStore.put(schema, false /* overwritten */);

    ColumnEntity column =
        ColumnEntity.builder()
            .withId(idGenerator.nextId())
            .withName(COLUMN)
            .withPosition(0)
            .withComment("Test column")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(audit)
            .build();

    TableEntity table =
        TableEntity.builder()
            .withId(idGenerator.nextId())
            .withName(TABLE)
            .withColumns(Lists.newArrayList(column))
            .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
            .withAuditInfo(audit)
            .build();
    entityStore.put(table, false /* overwritten */);

    tagManager = new TagManager(idGenerator, entityStore);

    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "metalakeDispatcher", metalakeDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", catalogDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "schemaDispatcher", schemaDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", tableDispatcher, true);

    when(metalakeDispatcher.metalakeExists(any())).thenReturn(true);
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);
    when(schemaDispatcher.schemaExists(any())).thenReturn(true);
    when(tableDispatcher.tableExists(any())).thenReturn(true);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }

    FileUtils.deleteDirectory(new File(JDBC_STORE_PATH));
  }

  @AfterEach
  public void cleanUp() {
    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    String[] catalogTags = tagManager.listTagsForMetadataObject(METALAKE, catalogObject);
    tagManager.associateTagsForMetadataObject(METALAKE, catalogObject, null, catalogTags);

    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    String[] schemaTags = tagManager.listTagsForMetadataObject(METALAKE, schemaObject);
    tagManager.associateTagsForMetadataObject(METALAKE, schemaObject, null, schemaTags);

    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    String[] tableTags = tagManager.listTagsForMetadataObject(METALAKE, tableObject);
    tagManager.associateTagsForMetadataObject(METALAKE, tableObject, null, tableTags);

    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);
    String[] columnTags = tagManager.listTagsForMetadataObject(METALAKE, columnObject);
    tagManager.associateTagsForMetadataObject(METALAKE, columnObject, null, columnTags);

    Arrays.stream(tagManager.listTags(METALAKE)).forEach(n -> tagManager.deleteTag(METALAKE, n));
  }

  @Test
  public void testCreateAndGetTag() {
    Tag tag = tagManager.createTag(METALAKE, "tag1", null, null);
    Assertions.assertEquals("tag1", tag.name());
    Assertions.assertNull(tag.comment());
    Assertions.assertTrue(tag.properties().isEmpty());

    Tag tag1 = tagManager.getTag(METALAKE, "tag1");
    Assertions.assertEquals(tag, tag1);

    // Create a tag in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> tagManager.createTag("non_existent_metalake", "tag1", null, null));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());

    // Create a existent tag
    e =
        Assertions.assertThrows(
            TagAlreadyExistsException.class,
            () -> tagManager.createTag(METALAKE, "tag1", null, null));
    Assertions.assertEquals(
        "Tag with name tag1 under metalake metalake_for_tag_test already exists", e.getMessage());

    // Get a non-existent tag
    e =
        Assertions.assertThrows(
            NoSuchTagException.class, () -> tagManager.getTag(METALAKE, "non_existent_tag"));
    Assertions.assertEquals(
        "Tag with name non_existent_tag under metalake metalake_for_tag_test does not exist",
        e.getMessage());
  }

  @Test
  public void testCreateAndListTags() {
    tagManager.createTag(METALAKE, "tag1", null, null);
    tagManager.createTag(METALAKE, "tag2", null, null);
    tagManager.createTag(METALAKE, "tag3", null, null);

    Set<String> tagNames = Arrays.stream(tagManager.listTags(METALAKE)).collect(Collectors.toSet());
    Assertions.assertEquals(3, tagNames.size());
    Set<String> expectedNames = ImmutableSet.of("tag1", "tag2", "tag3");
    Assertions.assertEquals(expectedNames, tagNames);

    // List tags in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> tagManager.listTags("non_existent_metalake"));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());
  }

  @Test
  public void testAlterTag() {
    String tagComment = "tag comment";
    Map<String, String> tagProp = ImmutableMap.of("k1", "k2");
    tagManager.createTag(METALAKE, "tag1", tagComment, tagProp);

    // Test rename tag
    TagChange rename = TagChange.rename("new_tag1");
    Tag renamedTag = tagManager.alterTag(METALAKE, "tag1", rename);
    Assertions.assertEquals("new_tag1", renamedTag.name());
    Assertions.assertEquals(tagComment, renamedTag.comment());
    Assertions.assertEquals(tagProp, renamedTag.properties());

    // Test change comment
    TagChange changeComment = TagChange.updateComment("new comment");
    Tag changedCommentTag = tagManager.alterTag(METALAKE, "new_tag1", changeComment);
    Assertions.assertEquals("new_tag1", changedCommentTag.name());
    Assertions.assertEquals("new comment", changedCommentTag.comment());
    Assertions.assertEquals(tagProp, changedCommentTag.properties());

    // Test add new property
    TagChange addProp = TagChange.setProperty("k2", "v2");
    Tag addedPropTag = tagManager.alterTag(METALAKE, "new_tag1", addProp);
    Assertions.assertEquals("new_tag1", addedPropTag.name());
    Assertions.assertEquals("new comment", addedPropTag.comment());
    Map<String, String> expectedProp = ImmutableMap.of("k1", "k2", "k2", "v2");
    Assertions.assertEquals(expectedProp, addedPropTag.properties());

    // Test update existing property
    TagChange updateProp = TagChange.setProperty("k1", "v1");
    Tag updatedPropTag = tagManager.alterTag(METALAKE, "new_tag1", updateProp);
    Assertions.assertEquals("new_tag1", updatedPropTag.name());
    Assertions.assertEquals("new comment", updatedPropTag.comment());
    Map<String, String> expectedProp1 = ImmutableMap.of("k1", "v1", "k2", "v2");
    Assertions.assertEquals(expectedProp1, updatedPropTag.properties());

    // Test remove property
    TagChange removeProp = TagChange.removeProperty("k1");
    Tag removedPropTag = tagManager.alterTag(METALAKE, "new_tag1", removeProp);
    Assertions.assertEquals("new_tag1", removedPropTag.name());
    Assertions.assertEquals("new comment", removedPropTag.comment());
    Map<String, String> expectedProp2 = ImmutableMap.of("k2", "v2");
    Assertions.assertEquals(expectedProp2, removedPropTag.properties());
  }

  @Test
  public void testDeleteTag() {
    tagManager.createTag(METALAKE, "tag1", null, null);
    Assertions.assertTrue(tagManager.deleteTag(METALAKE, "tag1"));

    // Delete a non-existent tag
    Assertions.assertFalse(tagManager.deleteTag(METALAKE, "non_existent_tag"));

    // Delete a tag in non-existent metalake
    Exception e =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> tagManager.deleteTag("non_existent_metalake", "tag1"));
    Assertions.assertEquals("Metalake non_existent_metalake does not exist", e.getMessage());
  }

  @Test
  public void testAssociateTagsForMetadataObject() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);
    Tag tag2 = tagManager.createTag(METALAKE, "tag2", null, null);
    Tag tag3 = tagManager.createTag(METALAKE, "tag3", null, null);

    // Test associate tags for catalog
    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    String[] tagsToAdd = new String[] {tag1.name(), tag2.name(), tag3.name()};

    String[] tags =
        tagManager.associateTagsForMetadataObject(METALAKE, catalogObject, tagsToAdd, null);

    Assertions.assertEquals(3, tags.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2", "tag3"), ImmutableSet.copyOf(tags));

    // Test disassociate tags for catalog
    String[] tagsToRemove = new String[] {tag1.name()};
    String[] tags1 =
        tagManager.associateTagsForMetadataObject(METALAKE, catalogObject, null, tagsToRemove);

    Assertions.assertEquals(2, tags1.length);
    Assertions.assertEquals(ImmutableSet.of("tag2", "tag3"), ImmutableSet.copyOf(tags1));

    // Test associate and disassociate no tags for catalog
    String[] tags2 = tagManager.associateTagsForMetadataObject(METALAKE, catalogObject, null, null);

    Assertions.assertEquals(2, tags2.length);
    Assertions.assertEquals(ImmutableSet.of("tag2", "tag3"), ImmutableSet.copyOf(tags2));

    // Test re-associate tags for catalog
    Throwable e =
        Assertions.assertThrows(
            TagAlreadyAssociatedException.class,
            () ->
                tagManager.associateTagsForMetadataObject(
                    METALAKE, catalogObject, tagsToAdd, null));
    Assertions.assertTrue(e.getMessage().contains("Failed to associate tags for metadata object"));

    // Test associate and disassociate non-existent tags for catalog
    String[] tags3 =
        tagManager.associateTagsForMetadataObject(
            METALAKE, catalogObject, new String[] {"tag4", "tag5"}, new String[] {"tag6"});

    Assertions.assertEquals(2, tags3.length);
    Assertions.assertEquals(ImmutableSet.of("tag2", "tag3"), ImmutableSet.copyOf(tags3));

    // Test associate tags for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e1 =
        Assertions.assertThrows(
            NotFoundException.class,
            () ->
                tagManager.associateTagsForMetadataObject(
                    METALAKE, nonExistentObject, tagsToAdd, null));
    Assertions.assertTrue(e1.getMessage().contains("Failed to associate tags for metadata object"));

    // Test associate tags for unsupported metadata object
    MetadataObject metalakeObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofMetalake(METALAKE), Entity.EntityType.METALAKE);
    Throwable e2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tagManager.associateTagsForMetadataObject(
                    METALAKE, metalakeObject, tagsToAdd, null));
    Assertions.assertTrue(
        e2.getMessage().contains("Cannot associate tags for unsupported metadata object type"));

    // Test associate tags for schema
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    String[] tags4 =
        tagManager.associateTagsForMetadataObject(METALAKE, schemaObject, tagsToAdd, null);

    Assertions.assertEquals(3, tags4.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2", "tag3"), ImmutableSet.copyOf(tags4));

    // Test associate tags for table
    String[] tagsToAdd1 = new String[] {tag1.name()};
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    String[] tags5 =
        tagManager.associateTagsForMetadataObject(METALAKE, tableObject, tagsToAdd1, null);

    Assertions.assertEquals(1, tags5.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags5));

    // Test associate and disassociate same tags for table
    String[] tagsToAdd2 = new String[] {tag2.name(), tag3.name()};
    String[] tagsToRemove1 = new String[] {tag2.name()};
    String[] tags6 =
        tagManager.associateTagsForMetadataObject(METALAKE, tableObject, tagsToAdd2, tagsToRemove1);

    Assertions.assertEquals(2, tags6.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag3"), ImmutableSet.copyOf(tags6));

    // Test associate and disassociate same tags for column
    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);

    String[] tagsToAdd3 = new String[] {tag1.name()};
    String[] tags7 =
        tagManager.associateTagsForMetadataObject(METALAKE, columnObject, tagsToAdd3, null);

    Assertions.assertEquals(1, tags7.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags7));

    // Test associate and disassociate tags for column
    String[] tagsToRemove2 = new String[] {tag1.name()};
    String[] tags8 =
        tagManager.associateTagsForMetadataObject(METALAKE, columnObject, null, tagsToRemove2);

    Assertions.assertEquals(0, tags8.length);
    Assertions.assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(tags8));

    // Test associate and disassociate same tags for column
    String[] tagsToAdd4 = new String[] {tag2.name(), tag3.name()};
    String[] tagsToRemove3 = new String[] {tag2.name()};
    String[] tags9 =
        tagManager.associateTagsForMetadataObject(
            METALAKE, columnObject, tagsToAdd4, tagsToRemove3);

    Assertions.assertEquals(1, tags9.length);
    Assertions.assertEquals(ImmutableSet.of("tag3"), ImmutableSet.copyOf(tags9));
  }

  @Test
  public void testListMetadataObjectsForTag() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);
    Tag tag2 = tagManager.createTag(METALAKE, "tag2", null, null);
    Tag tag3 = tagManager.createTag(METALAKE, "tag3", null, null);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);

    tagManager.associateTagsForMetadataObject(
        METALAKE, catalogObject, new String[] {tag1.name(), tag2.name(), tag3.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, schemaObject, new String[] {tag1.name(), tag2.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, tableObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, columnObject, new String[] {tag1.name()}, null);

    MetadataObject[] objects = tagManager.listMetadataObjectsForTag(METALAKE, tag1.name());
    Assertions.assertEquals(4, objects.length);
    Assertions.assertEquals(
        ImmutableSet.of(catalogObject, schemaObject, tableObject, columnObject),
        ImmutableSet.copyOf(objects));

    MetadataObject[] objects1 = tagManager.listMetadataObjectsForTag(METALAKE, tag2.name());
    Assertions.assertEquals(2, objects1.length);
    Assertions.assertEquals(
        ImmutableSet.of(catalogObject, schemaObject), ImmutableSet.copyOf(objects1));

    MetadataObject[] objects2 = tagManager.listMetadataObjectsForTag(METALAKE, tag3.name());
    Assertions.assertEquals(1, objects2.length);
    Assertions.assertEquals(ImmutableSet.of(catalogObject), ImmutableSet.copyOf(objects2));

    // List metadata objects for non-existent tag
    Throwable e =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () -> tagManager.listMetadataObjectsForTag(METALAKE, "non_existent_tag"));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Tag with name non_existent_tag under metalake " + METALAKE + " does not exist"));
  }

  @Test
  public void testListTagsForMetadataObject() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);
    Tag tag2 = tagManager.createTag(METALAKE, "tag2", null, null);
    Tag tag3 = tagManager.createTag(METALAKE, "tag3", null, null);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);

    tagManager.associateTagsForMetadataObject(
        METALAKE, catalogObject, new String[] {tag1.name(), tag2.name(), tag3.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, schemaObject, new String[] {tag1.name(), tag2.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, tableObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, columnObject, new String[] {tag1.name()}, null);

    String[] tags = tagManager.listTagsForMetadataObject(METALAKE, catalogObject);
    Assertions.assertEquals(3, tags.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2", "tag3"), ImmutableSet.copyOf(tags));

    Tag[] tagsInfo = tagManager.listTagsInfoForMetadataObject(METALAKE, catalogObject);
    Assertions.assertEquals(3, tagsInfo.length);
    Assertions.assertEquals(ImmutableSet.of(tag1, tag2, tag3), ImmutableSet.copyOf(tagsInfo));

    String[] tags1 = tagManager.listTagsForMetadataObject(METALAKE, schemaObject);
    Assertions.assertEquals(2, tags1.length);
    Assertions.assertEquals(ImmutableSet.of("tag1", "tag2"), ImmutableSet.copyOf(tags1));

    Tag[] tagsInfo1 = tagManager.listTagsInfoForMetadataObject(METALAKE, schemaObject);
    Assertions.assertEquals(2, tagsInfo1.length);
    Assertions.assertEquals(ImmutableSet.of(tag1, tag2), ImmutableSet.copyOf(tagsInfo1));

    String[] tags2 = tagManager.listTagsForMetadataObject(METALAKE, tableObject);
    Assertions.assertEquals(1, tags2.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags2));

    Tag[] tagsInfo2 = tagManager.listTagsInfoForMetadataObject(METALAKE, tableObject);
    Assertions.assertEquals(1, tagsInfo2.length);
    Assertions.assertEquals(ImmutableSet.of(tag1), ImmutableSet.copyOf(tagsInfo2));

    String[] tags3 = tagManager.listTagsForMetadataObject(METALAKE, columnObject);
    Assertions.assertEquals(1, tags3.length);
    Assertions.assertEquals(ImmutableSet.of("tag1"), ImmutableSet.copyOf(tags3));

    Tag[] tagsInfo3 = tagManager.listTagsInfoForMetadataObject(METALAKE, columnObject);
    Assertions.assertEquals(1, tagsInfo3.length);
    Assertions.assertEquals(ImmutableSet.of(tag1), ImmutableSet.copyOf(tagsInfo3));

    // List tags for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e =
        Assertions.assertThrows(
            NotFoundException.class,
            () -> tagManager.listTagsForMetadataObject(METALAKE, nonExistentObject));
    Assertions.assertTrue(
        e.getMessage().contains("Failed to list tags for metadata object " + nonExistentObject));
  }

  @Test
  public void testGetTagForMetadataObject() {
    Tag tag1 = tagManager.createTag(METALAKE, "tag1", null, null);
    Tag tag2 = tagManager.createTag(METALAKE, "tag2", null, null);
    Tag tag3 = tagManager.createTag(METALAKE, "tag3", null, null);

    MetadataObject catalogObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, CATALOG), Entity.EntityType.CATALOG);
    MetadataObject schemaObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA), Entity.EntityType.SCHEMA);
    MetadataObject tableObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTable(METALAKE, CATALOG, SCHEMA, TABLE), Entity.EntityType.TABLE);
    MetadataObject columnObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofColumn(METALAKE, CATALOG, SCHEMA, TABLE, COLUMN),
            Entity.EntityType.COLUMN);

    tagManager.associateTagsForMetadataObject(
        METALAKE, catalogObject, new String[] {tag1.name(), tag2.name(), tag3.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, schemaObject, new String[] {tag1.name(), tag2.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, tableObject, new String[] {tag1.name()}, null);
    tagManager.associateTagsForMetadataObject(
        METALAKE, columnObject, new String[] {tag1.name()}, null);

    Tag result = tagManager.getTagForMetadataObject(METALAKE, catalogObject, tag1.name());
    Assertions.assertEquals(tag1, result);

    Tag result1 = tagManager.getTagForMetadataObject(METALAKE, schemaObject, tag1.name());
    Assertions.assertEquals(tag1, result1);

    Tag result2 = tagManager.getTagForMetadataObject(METALAKE, schemaObject, tag2.name());
    Assertions.assertEquals(tag2, result2);

    Tag result3 = tagManager.getTagForMetadataObject(METALAKE, catalogObject, tag3.name());
    Assertions.assertEquals(tag3, result3);

    Tag result4 = tagManager.getTagForMetadataObject(METALAKE, tableObject, tag1.name());
    Assertions.assertEquals(tag1, result4);

    // Test get non-existent tag for metadata object
    Throwable e =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () -> tagManager.getTagForMetadataObject(METALAKE, catalogObject, "non_existent_tag"));
    Assertions.assertTrue(e.getMessage().contains("Tag non_existent_tag does not exist"));

    Throwable e1 =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () -> tagManager.getTagForMetadataObject(METALAKE, schemaObject, tag3.name()));
    Assertions.assertTrue(e1.getMessage().contains("Tag tag3 does not exist"));

    Throwable e2 =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () -> tagManager.getTagForMetadataObject(METALAKE, tableObject, tag2.name()));
    Assertions.assertTrue(e2.getMessage().contains("Tag tag2 does not exist"));

    // Test get tag for non-existent metadata object
    MetadataObject nonExistentObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog(METALAKE, "non_existent_catalog"),
            Entity.EntityType.CATALOG);
    Throwable e3 =
        Assertions.assertThrows(
            NotFoundException.class,
            () -> tagManager.getTagForMetadataObject(METALAKE, nonExistentObject, tag1.name()));
    Assertions.assertTrue(
        e3.getMessage().contains("Failed to get tag for metadata object " + nonExistentObject));
  }
}
