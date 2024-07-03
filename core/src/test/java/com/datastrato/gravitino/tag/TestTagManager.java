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
package com.datastrato.gravitino.tag;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static com.datastrato.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static com.datastrato.gravitino.Configs.VERSION_RETENTION_COUNT;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchTagException;
import com.datastrato.gravitino.exceptions.TagAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
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

  private static EntityStore entityStore;

  private static IdGenerator idGenerator;

  private static TagManager tagManager;

  @BeforeAll
  public static void setUp() throws IOException {
    idGenerator = new RandomIdGenerator();

    File dbDir = new File(DB_DIR);
    dbDir.mkdirs();

    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);

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

    tagManager = new TagManager(idGenerator, entityStore);
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
}
