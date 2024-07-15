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
package com.apache.gravitino.catalog;

import static com.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static com.apache.gravitino.StringIdentifier.ID_KEY;
import static com.apache.gravitino.TestBasePropertiesMetadata.COMMENT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.apache.gravitino.Config;
import com.apache.gravitino.Entity;
import com.apache.gravitino.GravitinoEnv;
import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.auth.AuthConstants;
import com.apache.gravitino.exceptions.NoSuchEntityException;
import com.apache.gravitino.lock.LockManager;
import com.apache.gravitino.messaging.Topic;
import com.apache.gravitino.messaging.TopicChange;
import com.apache.gravitino.meta.AuditInfo;
import com.apache.gravitino.meta.TopicEntity;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTopicOperationDispatcher extends TestOperationDispatcher {

  static SchemaOperationDispatcher schemaOperationDispatcher;
  static TopicOperationDispatcher topicOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    topicOperationDispatcher =
        new TopicOperationDispatcher(catalogManager, entityStore, idGenerator);

    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "schemaDispatcher", schemaOperationDispatcher, true);
  }

  @Test
  public void testCreateAndListTopics() {
    Namespace topicNs = Namespace.of(metalake, catalog, "schema121");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    NameIdentifier topicIdent1 = NameIdentifier.of(topicNs, "topic1");
    Topic topic1 = topicOperationDispatcher.createTopic(topicIdent1, "comment", null, props);
    Assertions.assertEquals("topic1", topic1.name());
    Assertions.assertEquals("comment", topic1.comment());
    testProperties(props, topic1.properties());

    NameIdentifier[] idents = topicOperationDispatcher.listTopics(topicNs);
    Assertions.assertEquals(1, idents.length);
    Assertions.assertEquals(topicIdent1, idents[0]);

    Map<String, String> illegalProps = ImmutableMap.of("k2", "v2");
    testPropertyException(
        () -> topicOperationDispatcher.createTopic(topicIdent1, "comment", null, illegalProps),
        "Properties are required and must be set");

    Map<String, String> illegalProps2 = ImmutableMap.of("k1", "v1", ID_KEY, "test");
    testPropertyException(
        () -> topicOperationDispatcher.createTopic(topicIdent1, "comment", null, illegalProps2),
        "Properties are reserved and cannot be set",
        "gravitino.identifier");
  }

  @Test
  public void testCreateAndLoadTopic() throws IOException {
    Namespace topicNs = Namespace.of(metalake, catalog, "schema131");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    NameIdentifier topicIdent1 = NameIdentifier.of(topicNs, "topic11");
    Topic topic1 = topicOperationDispatcher.createTopic(topicIdent1, "comment", null, props);
    Assertions.assertEquals("topic11", topic1.name());
    Assertions.assertEquals("comment", topic1.comment());
    testProperties(props, topic1.properties());

    Topic loadedTopic1 = topicOperationDispatcher.loadTopic(topicIdent1);
    Assertions.assertEquals(topic1.name(), loadedTopic1.name());
    Assertions.assertEquals(topic1.comment(), loadedTopic1.comment());
    testProperties(props, loadedTopic1.properties());
    // Audit info is gotten from the entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, loadedTopic1.auditInfo().creator());

    // Case 2: Test if the topic entity is not found in the entity store
    reset(entityStore);
    entityStore.delete(topicIdent1, Entity.EntityType.TOPIC);
    entityStore.delete(NameIdentifier.of(topicNs.levels()), Entity.EntityType.SCHEMA);
    doThrow(new NoSuchEntityException("")).when(entityStore).get(any(), any(), any());
    Topic loadedTopic2 = topicOperationDispatcher.loadTopic(topicIdent1);
    // Succeed to import the topic entity
    Assertions.assertTrue(entityStore.exists(topicIdent1, Entity.EntityType.TOPIC));
    Assertions.assertTrue(
        entityStore.exists(NameIdentifier.of(topicNs.levels()), Entity.EntityType.SCHEMA));
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTopic2.auditInfo().creator());

    // Case 3: Test if the entity store is failed to get the topic entity
    reset(entityStore);
    entityStore.delete(topicIdent1, Entity.EntityType.TOPIC);
    entityStore.delete(NameIdentifier.of(topicNs.levels()), Entity.EntityType.SCHEMA);
    doThrow(new IOException()).when(entityStore).get(any(), any(), any());
    Topic loadedTopic3 = topicOperationDispatcher.loadTopic(topicIdent1);
    // Succeed to import the topic entity
    Assertions.assertTrue(
        entityStore.exists(NameIdentifier.of(topicNs.levels()), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(entityStore.exists(topicIdent1, Entity.EntityType.TOPIC));
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTopic3.auditInfo().creator());

    // Case 4: Test if the topic entity is not matched
    reset(entityStore);
    TopicEntity unmatchedEntity =
        TopicEntity.builder()
            .withId(1L)
            .withName("topic11")
            .withNamespace(topicNs)
            .withAuditInfo(
                AuditInfo.builder().withCreator("gravitino").withCreateTime(Instant.now()).build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).get(any(), any(), any());
    Topic loadedTopic4 = topicOperationDispatcher.loadTopic(topicIdent1);
    // Succeed to import the topic entity
    reset(entityStore);
    TopicEntity topicEntity =
        entityStore.get(topicIdent1, Entity.EntityType.TOPIC, TopicEntity.class);
    Assertions.assertEquals("test", topicEntity.auditInfo().creator());
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTopic4.auditInfo().creator());
  }

  @Test
  public void testCreateAndAlterTopic() throws IOException {
    Namespace topicNs = Namespace.of(metalake, catalog, "schema141");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    NameIdentifier topicIdent = NameIdentifier.of(topicNs, "topic21");
    Topic topic = topicOperationDispatcher.createTopic(topicIdent, "comment", null, props);

    TopicChange[] changes =
        new TopicChange[] {TopicChange.setProperty("k3", "v3"), TopicChange.removeProperty("k1")};

    Topic alteredTopic = topicOperationDispatcher.alterTopic(topicIdent, changes);
    Assertions.assertEquals(topic.name(), alteredTopic.name());
    Assertions.assertEquals(topic.comment(), alteredTopic.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredTopic.properties());
    // Audit info is gotten from gravitino entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTopic.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTopic.auditInfo().lastModifier());

    // Case 2: Test if the topic entity is not found in the entity store
    reset(entityStore);
    doThrow(new NoSuchEntityException("")).when(entityStore).update(any(), any(), any(), any());
    Topic alteredTopic2 = topicOperationDispatcher.alterTopic(topicIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTopic2.auditInfo().creator());
    Assertions.assertEquals("test", alteredTopic2.auditInfo().lastModifier());

    // Case 3: Test if the entity store is failed to update the topic entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).update(any(), any(), any(), any());
    Topic alteredTopic3 = topicOperationDispatcher.alterTopic(topicIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTopic3.auditInfo().creator());
    Assertions.assertEquals("test", alteredTopic3.auditInfo().lastModifier());

    // Case 4: Test if the topic entity is not matched
    reset(entityStore);
    TopicEntity unmatchedEntity =
        TopicEntity.builder()
            .withId(1L)
            .withName("topic21")
            .withNamespace(topicNs)
            .withAuditInfo(
                AuditInfo.builder().withCreator("gravitino").withCreateTime(Instant.now()).build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).update(any(), any(), any(), any());
    Topic alteredTopic4 = topicOperationDispatcher.alterTopic(topicIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTopic4.auditInfo().creator());
    Assertions.assertEquals("test", alteredTopic4.auditInfo().lastModifier());

    // Test immutable topic properties
    TopicChange[] illegalChange =
        new TopicChange[] {TopicChange.setProperty(COMMENT_KEY, "new comment")};
    testPropertyException(
        () -> topicOperationDispatcher.alterTopic(topicIdent, illegalChange),
        "Property comment is immutable or reserved, cannot be set");
  }

  @Test
  public void testCreateAndDropTopic() throws IOException {
    Namespace topicNs = Namespace.of(metalake, catalog, "schema151");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    NameIdentifier topicIdent = NameIdentifier.of(topicNs, "topic31");
    Topic topic = topicOperationDispatcher.createTopic(topicIdent, "comment", null, props);
    Assertions.assertEquals("topic31", topic.name());
    Assertions.assertEquals("comment", topic.comment());
    testProperties(props, topic.properties());

    boolean dropped = topicOperationDispatcher.dropTopic(topicIdent);
    Assertions.assertTrue(dropped);
    Assertions.assertFalse(topicOperationDispatcher.dropTopic(topicIdent));

    // Test if the entity store is failed to drop the topic entity
    topicOperationDispatcher.createTopic(topicIdent, "comment", null, props);
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).delete(any(), any(), anyBoolean());
    Assertions.assertThrows(
        RuntimeException.class, () -> topicOperationDispatcher.dropTopic(topicIdent));
  }
}
