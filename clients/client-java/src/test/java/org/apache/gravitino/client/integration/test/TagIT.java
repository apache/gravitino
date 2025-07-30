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
package org.apache.gravitino.client.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Tag("gravitino-docker-test")
public class TagIT extends BaseIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String metalakeName = GravitinoITUtils.genRandomName("tag_it_metalake");

  private static GravitinoMetalake metalake;
  private static Catalog relationalCatalog;
  private static Schema schema;
  private static Table table;

  private static Catalog modelCatalog;
  private static Schema modelSchema;
  private static Model model;

  private static Column column;

  @BeforeAll
  public void setUp() {
    containerSuite.startHiveContainer();
    String hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    // Create metalake
    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake", Collections.emptyMap());

    // Create catalog
    String catalogName = GravitinoITUtils.genRandomName("tag_it_catalog");
    Assertions.assertFalse(metalake.catalogExists(catalogName));
    relationalCatalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            "hive",
            "comment",
            ImmutableMap.of("metastore.uris", hmsUri));

    // Create schema
    String schemaName = GravitinoITUtils.genRandomName("tag_it_schema");
    Assertions.assertFalse(relationalCatalog.asSchemas().schemaExists(schemaName));
    schema =
        relationalCatalog.asSchemas().createSchema(schemaName, "comment", Collections.emptyMap());

    // Create table
    String tableName = GravitinoITUtils.genRandomName("tag_it_table");
    Assertions.assertFalse(
        relationalCatalog.asTableCatalog().tableExists(NameIdentifier.of(schemaName, tableName)));
    table =
        relationalCatalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, tableName),
                new Column[] {
                  Column.of("col1", Types.IntegerType.get()),
                  Column.of("col2", Types.StringType.get())
                },
                "comment",
                Collections.emptyMap());
    column = Arrays.stream(table.columns()).filter(c -> c.name().equals("col1")).findFirst().get();

    // Create model catalog
    String modelCatalogName = GravitinoITUtils.genRandomName("tag_it_model_catalog");
    Assertions.assertFalse(metalake.catalogExists(modelCatalogName));
    modelCatalog =
        metalake.createCatalog(
            modelCatalogName, Catalog.Type.MODEL, "comment", Collections.emptyMap());

    // Create model schema
    String modelSchemaName = GravitinoITUtils.genRandomName("tag_it_model_schema");
    Assertions.assertFalse(modelCatalog.asSchemas().schemaExists(modelSchemaName));
    modelSchema =
        modelCatalog.asSchemas().createSchema(modelSchemaName, "comment", Collections.emptyMap());

    // Create model
    String modelName = GravitinoITUtils.genRandomName("tag_it_model");
    Assertions.assertFalse(
        modelCatalog.asModelCatalog().modelExists(NameIdentifier.of(modelSchemaName, modelName)));
    model =
        modelCatalog
            .asModelCatalog()
            .registerModel(
                NameIdentifier.of(modelSchemaName, modelName), "comment", Collections.emptyMap());
  }

  @AfterAll
  public void tearDown() {
    relationalCatalog.asTableCatalog().dropTable(NameIdentifier.of(schema.name(), table.name()));
    relationalCatalog.asSchemas().dropSchema(schema.name(), true);
    metalake.dropCatalog(relationalCatalog.name(), true);

    modelCatalog.asModelCatalog().deleteModel(NameIdentifier.of(modelSchema.name(), model.name()));
    modelCatalog.asSchemas().dropSchema(modelSchema.name(), true);
    metalake.dropCatalog(modelCatalog.name(), true);

    client.dropMetalake(metalakeName, true);

    if (client != null) {
      client.close();
      client = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      // Swallow exceptions
    }
  }

  @AfterEach
  public void cleanUp() {
    String[] tableTags = table.supportsTags().listTags();
    table.supportsTags().associateTags(null, tableTags);

    String[] schemaTags = schema.supportsTags().listTags();
    schema.supportsTags().associateTags(null, schemaTags);

    String[] catalogTags = relationalCatalog.supportsTags().listTags();
    relationalCatalog.supportsTags().associateTags(null, catalogTags);

    String[] tags = metalake.listTags();
    for (String tag : tags) {
      metalake.deleteTag(tag);
    }
  }

  @Test
  public void testCreateGetAndListTag() {
    String tagName = GravitinoITUtils.genRandomName("tag_it_tag");
    Assertions.assertThrows(NoSuchTagException.class, () -> metalake.getTag(tagName));

    // Test create
    Tag tag = metalake.createTag(tagName, "comment", Collections.emptyMap());
    Assertions.assertEquals(tagName, tag.name());
    Assertions.assertEquals("comment", tag.comment());
    Assertions.assertEquals(Collections.emptyMap(), tag.properties());
    Assertions.assertFalse(tag.inherited().isPresent());

    // Test already existed tag
    Assertions.assertThrows(
        TagAlreadyExistsException.class,
        () -> metalake.createTag(tagName, "comment", Collections.emptyMap()));

    // Test get
    Tag fetchedTag = metalake.getTag(tagName);
    Assertions.assertEquals(tag, fetchedTag);

    // test List names
    String tagName1 = GravitinoITUtils.genRandomName("tag_it_tag1");
    Tag tag1 = metalake.createTag(tagName1, "comment1", Collections.emptyMap());
    Assertions.assertEquals(tagName1, tag1.name());
    Assertions.assertEquals("comment1", tag1.comment());
    Assertions.assertEquals(Collections.emptyMap(), tag1.properties());
    Assertions.assertFalse(tag1.inherited().isPresent());

    String[] tagNames = metalake.listTags();
    Assertions.assertEquals(2, tagNames.length);
    Set<String> tagNamesSet = Sets.newHashSet(tagName, tagName1);
    Set<String> resultTagNamesSet = Sets.newHashSet(tagNames);
    Assertions.assertEquals(tagNamesSet, resultTagNamesSet);

    // test List tags
    Set<Tag> tags = Sets.newHashSet(metalake.listTagsInfo());
    Set<Tag> expectedTags = Sets.newHashSet(tag, tag1);
    Assertions.assertEquals(expectedTags, tags);

    // Test null comment and properties
    String tagName2 = GravitinoITUtils.genRandomName("tag_it_tag2");
    Tag tag2 = metalake.createTag(tagName2, null, null);
    Assertions.assertEquals(tagName2, tag2.name());
    Assertions.assertNull(tag2.comment());
    Assertions.assertEquals(Collections.emptyMap(), tag2.properties());

    Tag tag3 = metalake.getTag(tagName2);
    Assertions.assertEquals(tag2, tag3);
  }

  @Test
  public void testNullableComment() {
    String tagName = GravitinoITUtils.genRandomName("tag_it_tag");
    metalake.createTag(tagName, "comment", Collections.emptyMap());
    Tag alteredTag6 = metalake.alterTag(tagName, TagChange.updateComment(null));
    Assertions.assertNull(alteredTag6.comment());
  }

  @Test
  public void testCreateAndAlterTag() {
    String tagName = GravitinoITUtils.genRandomName("tag_it_tag");
    metalake.createTag(tagName, "comment", Collections.emptyMap());

    // Test rename and update comment
    String newTagName = GravitinoITUtils.genRandomName("tag_it_tag_new");
    TagChange rename = TagChange.rename(newTagName);
    TagChange updateComment = TagChange.updateComment("new comment");

    Tag alteredTag = metalake.alterTag(tagName, rename, updateComment);
    Assertions.assertEquals(newTagName, alteredTag.name());
    Assertions.assertEquals("new comment", alteredTag.comment());
    Assertions.assertEquals(Collections.emptyMap(), alteredTag.properties());
    Assertions.assertFalse(alteredTag.inherited().isPresent());

    // Test set properties
    TagChange setProperty = TagChange.setProperty("k1", "v1");
    Tag alteredTag1 = metalake.alterTag(newTagName, setProperty);
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), alteredTag1.properties());
    Assertions.assertFalse(alteredTag1.inherited().isPresent());

    // Test remove properties
    TagChange removeProperty = TagChange.removeProperty("k1");
    Tag alteredTag2 = metalake.alterTag(newTagName, removeProperty);
    Assertions.assertEquals(Collections.emptyMap(), alteredTag2.properties());
    Assertions.assertFalse(alteredTag2.inherited().isPresent());

    // Test set and remove same property
    TagChange setProperty1 = TagChange.setProperty("k2", "v2");
    TagChange removeProperty1 = TagChange.removeProperty("k2");
    Tag alteredTag3 = metalake.alterTag(newTagName, setProperty1, removeProperty1);
    Assertions.assertEquals(Collections.emptyMap(), alteredTag3.properties());
    Assertions.assertFalse(alteredTag3.inherited().isPresent());

    // Test remove non-existed property
    TagChange setProperty2 = TagChange.setProperty("k3", "v3");
    TagChange removeProperty2 = TagChange.removeProperty("k4");
    Tag alteredTag4 = metalake.alterTag(newTagName, setProperty2, removeProperty2);
    Assertions.assertEquals(ImmutableMap.of("k3", "v3"), alteredTag4.properties());

    // Test throw NoSuchTagException
    Assertions.assertThrows(
        NoSuchTagException.class, () -> metalake.alterTag("non-existed-tag", rename));

    // Test alter tag on no comment and properties
    String tagName1 = GravitinoITUtils.genRandomName("tag_it_tag1");
    metalake.createTag(tagName1, null, null);

    // Test rename and update comment
    String newTagName1 = GravitinoITUtils.genRandomName("tag_it_tag_new1");
    TagChange rename1 = TagChange.rename(newTagName1);
    TagChange updateComment1 = TagChange.updateComment("new comment1");
    TagChange setProperty3 = TagChange.setProperty("k4", "v4");
    TagChange setProperty4 = TagChange.setProperty("k5", "v5");
    TagChange removeProperty3 = TagChange.removeProperty("k4");

    Tag alteredTag5 =
        metalake.alterTag(
            tagName1, rename1, updateComment1, setProperty3, setProperty4, removeProperty3);
    Assertions.assertEquals(newTagName1, alteredTag5.name());
    Assertions.assertEquals("new comment1", alteredTag5.comment());
    Assertions.assertEquals(ImmutableMap.of("k5", "v5"), alteredTag5.properties());
    Assertions.assertFalse(alteredTag5.inherited().isPresent());
  }

  @Test
  public void testCreateAndDeleteTag() {
    String tagName = GravitinoITUtils.genRandomName("tag_it_tag");
    metalake.createTag(tagName, "comment", Collections.emptyMap());

    // Test delete
    Assertions.assertTrue(metalake.deleteTag(tagName));
    Assertions.assertFalse(metalake.deleteTag(tagName));
  }

  @Test
  public void testAssociateTagsToCatalog() {
    Tag tag1 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_catalog_tag1"),
            "comment1",
            Collections.emptyMap());
    Tag tag2 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_catalog_tag2"),
            "comment2",
            Collections.emptyMap());

    // Test associate tags to catalog
    String[] tags =
        relationalCatalog
            .supportsTags()
            .associateTags(new String[] {tag1.name(), tag2.name()}, null);

    Assertions.assertEquals(2, tags.length);
    Set<String> tagNames = Sets.newHashSet(tags);
    Assertions.assertTrue(tagNames.contains(tag1.name()));
    Assertions.assertTrue(tagNames.contains(tag2.name()));

    // Test disassociate tags from catalog
    String[] tags1 =
        relationalCatalog
            .supportsTags()
            .associateTags(null, new String[] {tag1.name(), tag2.name()});
    Assertions.assertEquals(0, tags1.length);

    // Test associate non-existed tags to catalog
    String[] tags2 =
        relationalCatalog.supportsTags().associateTags(new String[] {"non-existed-tag"}, null);
    Assertions.assertEquals(0, tags2.length);

    // Test disassociate non-existed tags from catalog
    String[] tags3 =
        relationalCatalog.supportsTags().associateTags(null, new String[] {"non-existed-tag"});
    Assertions.assertEquals(0, tags3.length);

    // Test associate same tags to catalog
    String[] tags4 =
        relationalCatalog
            .supportsTags()
            .associateTags(new String[] {tag1.name(), tag1.name()}, null);
    Assertions.assertEquals(1, tags4.length);
    Assertions.assertEquals(tag1.name(), tags4[0]);

    // Test associate same tag again to catalog
    Assertions.assertThrows(
        TagAlreadyAssociatedException.class,
        () -> relationalCatalog.supportsTags().associateTags(new String[] {tag1.name()}, null));

    // Test associate and disassociate same tags to catalog
    String[] tags5 =
        relationalCatalog
            .supportsTags()
            .associateTags(new String[] {tag2.name()}, new String[] {tag2.name()});
    Assertions.assertEquals(1, tags5.length);
    Assertions.assertEquals(tag1.name(), tags5[0]);

    // Test List associated tags for catalog
    String[] tags6 = relationalCatalog.supportsTags().listTags();
    Assertions.assertEquals(1, tags6.length);
    Assertions.assertEquals(tag1.name(), tags6[0]);

    // Test List associated tags with details for catalog
    Tag[] tags7 = relationalCatalog.supportsTags().listTagsInfo();
    Assertions.assertEquals(1, tags7.length);
    Assertions.assertEquals(tag1, tags7[0]);
    Assertions.assertFalse(tags7[0].inherited().get());

    // Test get associated tag for catalog
    Tag tag = relationalCatalog.supportsTags().getTag(tag1.name());
    Assertions.assertEquals(tag1, tag);
    Assertions.assertFalse(tag.inherited().get());

    // Test get non-existed tag for catalog
    Assertions.assertThrows(
        NoSuchTagException.class, () -> relationalCatalog.supportsTags().getTag("non-existed-tag"));

    // Test get objects associated with tag
    Assertions.assertEquals(1, tag.associatedObjects().count());
    MetadataObject catalogObject = tag.associatedObjects().objects()[0];
    Assertions.assertEquals(relationalCatalog.name(), catalogObject.name());
    Assertions.assertEquals(MetadataObject.Type.CATALOG, catalogObject.type());
  }

  @Test
  public void testAssociateTagsToSchema() {
    Tag tag1 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_schema_tag1"),
            "comment1",
            Collections.emptyMap());
    Tag tag2 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_schema_tag2"),
            "comment2",
            Collections.emptyMap());

    // Associate tags to catalog
    relationalCatalog.supportsTags().associateTags(new String[] {tag1.name()}, null);

    // Test associate tags to schema
    String[] tags =
        schema.supportsTags().associateTags(new String[] {tag1.name(), tag2.name()}, null);

    Assertions.assertEquals(2, tags.length);
    Set<String> tagNames = Sets.newHashSet(tags);
    Assertions.assertTrue(tagNames.contains(tag1.name()));
    Assertions.assertTrue(tagNames.contains(tag2.name()));

    // Test list associated tags for schema
    String[] tags1 = schema.supportsTags().listTags();
    Assertions.assertEquals(2, tags1.length);

    // Test list associated tags with details for schema
    Tag[] tags2 = schema.supportsTags().listTagsInfo();
    Assertions.assertEquals(2, tags2.length);

    Set<Tag> nonInheritedTags =
        Arrays.stream(tags2).filter(tag -> !tag.inherited().get()).collect(Collectors.toSet());
    Set<Tag> inheritedTags =
        Arrays.stream(tags2).filter(tag -> tag.inherited().get()).collect(Collectors.toSet());

    Assertions.assertEquals(2, nonInheritedTags.size());
    Assertions.assertEquals(0, inheritedTags.size());
    Assertions.assertTrue(nonInheritedTags.contains(tag1));
    Assertions.assertTrue(nonInheritedTags.contains(tag2));
    Assertions.assertFalse(inheritedTags.contains(tag2));

    // Test get associated tag for schema
    Tag tag = schema.supportsTags().getTag(tag1.name());
    Assertions.assertEquals(tag1, tag);
    Assertions.assertFalse(tag.inherited().get());

    // Test get objects associated with tag
    Assertions.assertEquals(2, tag.associatedObjects().count());
    Set<MetadataObject> resultObjects = Sets.newHashSet(tag.associatedObjects().objects());
    Set<MetadataObject> expectedObjects =
        Sets.newHashSet(
            MetadataObjectDTO.builder()
                .withName(relationalCatalog.name())
                .withType(MetadataObject.Type.CATALOG)
                .build(),
            MetadataObjectDTO.builder()
                .withParent(relationalCatalog.name())
                .withName(schema.name())
                .withType(MetadataObject.Type.SCHEMA)
                .build());
    Assertions.assertEquals(expectedObjects, resultObjects);
  }

  @Test
  public void testAssociateTagsToTable() {
    Tag tag1 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_table_tag1"),
            "comment1",
            Collections.emptyMap());
    Tag tag2 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_table_tag2"),
            "comment2",
            Collections.emptyMap());
    Tag tag3 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_table_tag3"),
            "comment3",
            Collections.emptyMap());

    // Associate tags to catalog
    relationalCatalog.supportsTags().associateTags(new String[] {tag1.name()}, null);

    // Associate tags to schema
    schema.supportsTags().associateTags(new String[] {tag2.name()}, null);

    // Test associate tags to table
    String[] tags = table.supportsTags().associateTags(new String[] {tag3.name()}, null);

    Assertions.assertEquals(1, tags.length);
    Assertions.assertEquals(tag3.name(), tags[0]);

    // Test list associated tags for table
    String[] tags1 = table.supportsTags().listTags();
    Assertions.assertEquals(3, tags1.length);
    Set<String> tagNames = Sets.newHashSet(tags1);
    Assertions.assertTrue(tagNames.contains(tag1.name()));
    Assertions.assertTrue(tagNames.contains(tag2.name()));
    Assertions.assertTrue(tagNames.contains(tag3.name()));

    // Test list associated tags with details for table
    Tag[] tags2 = table.supportsTags().listTagsInfo();
    Assertions.assertEquals(3, tags2.length);

    Set<Tag> nonInheritedTags =
        Arrays.stream(tags2).filter(tag -> !tag.inherited().get()).collect(Collectors.toSet());
    Set<Tag> inheritedTags =
        Arrays.stream(tags2).filter(tag -> tag.inherited().get()).collect(Collectors.toSet());

    Assertions.assertEquals(1, nonInheritedTags.size());
    Assertions.assertEquals(2, inheritedTags.size());
    Assertions.assertTrue(nonInheritedTags.contains(tag3));
    Assertions.assertTrue(inheritedTags.contains(tag1));
    Assertions.assertTrue(inheritedTags.contains(tag2));

    // Test get associated tag for table
    Tag resultTag1 = table.supportsTags().getTag(tag1.name());
    Assertions.assertEquals(tag1, resultTag1);
    Assertions.assertTrue(resultTag1.inherited().get());

    Tag resultTag2 = table.supportsTags().getTag(tag2.name());
    Assertions.assertEquals(tag2, resultTag2);
    Assertions.assertTrue(resultTag2.inherited().get());

    Tag resultTag3 = table.supportsTags().getTag(tag3.name());
    Assertions.assertEquals(tag3, resultTag3);
    Assertions.assertFalse(resultTag3.inherited().get());

    // Test get objects associated with tag
    Assertions.assertEquals(1, tag1.associatedObjects().count());
    Assertions.assertEquals(relationalCatalog.name(), tag1.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG, tag1.associatedObjects().objects()[0].type());

    Assertions.assertEquals(1, tag2.associatedObjects().count());
    Assertions.assertEquals(schema.name(), tag2.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.SCHEMA, tag2.associatedObjects().objects()[0].type());

    Assertions.assertEquals(1, tag3.associatedObjects().count());
    Assertions.assertEquals(table.name(), tag3.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.TABLE, tag3.associatedObjects().objects()[0].type());
  }

  @Test
  public void testAssociateTagsToColumn() {
    Tag tag1 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_column_tag1"),
            "comment1",
            Collections.emptyMap());
    Tag tag2 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_column_tag2"),
            "comment2",
            Collections.emptyMap());
    Tag tag3 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_column_tag3"),
            "comment3",
            Collections.emptyMap());
    Tag tag4 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_column_tag4"),
            "comment4",
            Collections.emptyMap());

    // Associate tags to catalog
    relationalCatalog.supportsTags().associateTags(new String[] {tag1.name()}, null);

    // Associate tags to schema
    schema.supportsTags().associateTags(new String[] {tag2.name()}, null);

    // Associate tags to table
    table.supportsTags().associateTags(new String[] {tag3.name()}, null);

    // Associate tags to column
    String[] tags = column.supportsTags().associateTags(new String[] {tag4.name()}, null);

    Assertions.assertEquals(1, tags.length);
    Set<String> tagNames = Sets.newHashSet(tags);
    Assertions.assertTrue(tagNames.contains(tag4.name()));

    // Test list associated tags for column
    String[] tags1 = column.supportsTags().listTags();
    Assertions.assertEquals(4, tags1.length);
    Set<String> tagNames1 = Sets.newHashSet(tags1);
    Assertions.assertTrue(tagNames1.contains(tag1.name()));
    Assertions.assertTrue(tagNames1.contains(tag2.name()));
    Assertions.assertTrue(tagNames1.contains(tag3.name()));
    Assertions.assertTrue(tagNames1.contains(tag4.name()));

    // Test list associated tags with details for column
    Tag[] tags2 = column.supportsTags().listTagsInfo();
    Assertions.assertEquals(4, tags2.length);

    Set<Tag> nonInheritedTags =
        Arrays.stream(tags2).filter(tag -> !tag.inherited().get()).collect(Collectors.toSet());
    Set<Tag> inheritedTags =
        Arrays.stream(tags2).filter(tag -> tag.inherited().get()).collect(Collectors.toSet());

    Assertions.assertEquals(1, nonInheritedTags.size());
    Assertions.assertEquals(3, inheritedTags.size());
    Assertions.assertTrue(nonInheritedTags.contains(tag4));
    Assertions.assertTrue(inheritedTags.contains(tag1));
    Assertions.assertTrue(inheritedTags.contains(tag2));
    Assertions.assertTrue(inheritedTags.contains(tag3));

    // Test get associated tag for column
    Tag resultTag1 = column.supportsTags().getTag(tag1.name());
    Assertions.assertEquals(tag1, resultTag1);
    Assertions.assertTrue(resultTag1.inherited().get());

    Tag resultTag2 = column.supportsTags().getTag(tag2.name());
    Assertions.assertEquals(tag2, resultTag2);
    Assertions.assertTrue(resultTag2.inherited().get());

    Tag resultTag3 = column.supportsTags().getTag(tag3.name());
    Assertions.assertEquals(tag3, resultTag3);
    Assertions.assertTrue(resultTag3.inherited().get());

    Tag resultTag4 = column.supportsTags().getTag(tag4.name());
    Assertions.assertEquals(tag4, resultTag4);
    Assertions.assertFalse(resultTag4.inherited().get());

    // Test get objects associated with tag
    Assertions.assertEquals(1, tag1.associatedObjects().count());
    Assertions.assertEquals(relationalCatalog.name(), tag1.associatedObjects().objects()[0].name());

    Assertions.assertEquals(1, tag2.associatedObjects().count());
    Assertions.assertEquals(schema.name(), tag2.associatedObjects().objects()[0].name());

    Assertions.assertEquals(1, tag3.associatedObjects().count());
    Assertions.assertEquals(table.name(), tag3.associatedObjects().objects()[0].name());

    Assertions.assertEquals(1, tag4.associatedObjects().count());
    Assertions.assertEquals(column.name(), tag4.associatedObjects().objects()[0].name());
  }

  @Test
  public void testAssociateAndDeleteTags() {
    Tag tag1 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_tag1"), "comment1", Collections.emptyMap());
    Tag tag2 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_tag2"), "comment2", Collections.emptyMap());
    Tag tag3 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_tag3"), "comment3", Collections.emptyMap());

    String[] associatedTags =
        relationalCatalog
            .supportsTags()
            .associateTags(new String[] {tag1.name(), tag2.name()}, new String[] {tag3.name()});

    Assertions.assertEquals(2, associatedTags.length);
    Set<String> tagNames = Sets.newHashSet(associatedTags);
    Assertions.assertTrue(tagNames.contains(tag1.name()));
    Assertions.assertTrue(tagNames.contains(tag2.name()));
    Assertions.assertFalse(tagNames.contains(tag3.name()));

    Tag retrievedTag = relationalCatalog.supportsTags().getTag(tag2.name());
    Assertions.assertEquals(tag2.name(), retrievedTag.name());
    Assertions.assertEquals(tag2.comment(), retrievedTag.comment());

    boolean deleted = metalake.deleteTag("null");
    Assertions.assertFalse(deleted);

    deleted = metalake.deleteTag(tag1.name());
    Assertions.assertTrue(deleted);
    deleted = metalake.deleteTag(tag1.name());
    Assertions.assertFalse(deleted);

    String[] associatedTags1 = relationalCatalog.supportsTags().listTags();
    Assertions.assertArrayEquals(new String[] {tag2.name()}, associatedTags1);
  }

  @Test
  public void testAssociateTagsToModel() {
    Tag tag1 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_model_tag1"),
            "comment1",
            Collections.emptyMap());
    Tag tag2 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_model_tag2"),
            "comment2",
            Collections.emptyMap());
    Tag tag3 =
        metalake.createTag(
            GravitinoITUtils.genRandomName("tag_it_model_tag3"),
            "comment3",
            Collections.emptyMap());

    // Associate tags to catalog
    modelCatalog.supportsTags().associateTags(new String[] {tag1.name()}, null);

    // Associate tags to schema
    modelSchema.supportsTags().associateTags(new String[] {tag2.name()}, null);

    // Associate tags to model
    model.supportsTags().associateTags(new String[] {tag3.name()}, null);

    // Test list associated tags for model
    String[] tags1 = model.supportsTags().listTags();
    Assertions.assertEquals(3, tags1.length);
    Set<String> tagNames = Sets.newHashSet(tags1);
    Assertions.assertTrue(tagNames.contains(tag1.name()));
    Assertions.assertTrue(tagNames.contains(tag2.name()));
    Assertions.assertTrue(tagNames.contains(tag3.name()));

    // Test list associated tags with details for model
    Tag[] tags2 = model.supportsTags().listTagsInfo();
    Assertions.assertEquals(3, tags2.length);

    Set<Tag> nonInheritedTags =
        Arrays.stream(tags2).filter(tag -> !tag.inherited().get()).collect(Collectors.toSet());
    Set<Tag> inheritedTags =
        Arrays.stream(tags2).filter(tag -> tag.inherited().get()).collect(Collectors.toSet());

    Assertions.assertEquals(1, nonInheritedTags.size());
    Assertions.assertEquals(2, inheritedTags.size());
    Assertions.assertTrue(nonInheritedTags.contains(tag3));
    Assertions.assertTrue(inheritedTags.contains(tag1));
    Assertions.assertTrue(inheritedTags.contains(tag2));

    // Test get associated tag for model
    Tag resultTag1 = model.supportsTags().getTag(tag1.name());
    Assertions.assertEquals(tag1, resultTag1);
    Assertions.assertTrue(resultTag1.inherited().get());

    Tag resultTag2 = model.supportsTags().getTag(tag2.name());
    Assertions.assertEquals(tag2, resultTag2);
    Assertions.assertTrue(resultTag2.inherited().get());

    Tag resultTag3 = model.supportsTags().getTag(tag3.name());
    Assertions.assertEquals(tag3, resultTag3);
    Assertions.assertFalse(resultTag3.inherited().get());

    // Test get objects associated with tag
    Assertions.assertEquals(1, tag1.associatedObjects().count());
    Assertions.assertEquals(modelCatalog.name(), tag1.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG, tag1.associatedObjects().objects()[0].type());

    Assertions.assertEquals(1, tag2.associatedObjects().count());
    Assertions.assertEquals(modelSchema.name(), tag2.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.SCHEMA, tag2.associatedObjects().objects()[0].type());

    Assertions.assertEquals(1, tag3.associatedObjects().count());
    Assertions.assertEquals(model.name(), tag3.associatedObjects().objects()[0].name());
    Assertions.assertEquals(
        MetadataObject.Type.MODEL, tag3.associatedObjects().objects()[0].type());
  }
}
