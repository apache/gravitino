/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.TestBasePropertiesMetadata.COMMENT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.TopicEntity;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTopicOperationDispatcher extends TestOperationDispatcher {

  private static SchemaOperationDispatcher schemaOperationDispatcher;
  private static TopicOperationDispatcher topicOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    topicOperationDispatcher =
        new TopicOperationDispatcher(catalogManager, entityStore, idGenerator);
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
    doThrow(new NoSuchEntityException("")).when(entityStore).get(any(), any(), any());
    Topic loadedTopic2 = topicOperationDispatcher.loadTopic(topicIdent1);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTopic2.auditInfo().creator());

    // Case 3: Test if the entity store is failed to get the topic entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).get(any(), any(), any());
    Topic loadedTopic3 = topicOperationDispatcher.loadTopic(topicIdent1);
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

    // Test if the entity store is failed to drop the topic entity
    topicOperationDispatcher.createTopic(topicIdent, "comment", null, props);
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).delete(any(), any(), anyBoolean());
    Assertions.assertThrows(
        RuntimeException.class, () -> topicOperationDispatcher.dropTopic(topicIdent));
  }
}
