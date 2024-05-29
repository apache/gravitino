/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.SECURABLE_ENTITY_RESERVED_NAME;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicChange;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTopicNormalizeDispatcher extends TestOperationDispatcher {
  private static TopicNormalizeDispatcher topicNormalizeDispatcher;
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    TestTopicOperationDispatcher.initialize();
    schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(TestTopicOperationDispatcher.schemaOperationDispatcher);
    topicNormalizeDispatcher =
        new TopicNormalizeDispatcher(TestTopicOperationDispatcher.topicOperationDispatcher);
  }

  @Test
  public void testNameCaseInsensitive() {
    Namespace topicNs = Namespace.of(metalake, catalog, "schema161");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    // test case-insensitive in creation
    NameIdentifier topicIdent = NameIdentifier.of(topicNs, "topicNAME");
    Topic createdTopic = topicNormalizeDispatcher.createTopic(topicIdent, "comment", null, props);
    Assertions.assertEquals(topicIdent.name().toLowerCase(), createdTopic.name());

    // test case-insensitive in loading
    Topic loadedTopic = topicNormalizeDispatcher.loadTopic(topicIdent);
    Assertions.assertEquals(topicIdent.name().toLowerCase(), loadedTopic.name());

    // test case-insensitive in listing
    NameIdentifier[] idents = topicNormalizeDispatcher.listTopics(topicNs);
    Assertions.assertEquals(topicIdent.name().toLowerCase(), idents[0].name());

    // test case-insensitive in altering
    Topic alteredTopic =
        topicNormalizeDispatcher.alterTopic(
            NameIdentifier.of(topicNs, topicIdent.name().toLowerCase()),
            TopicChange.setProperty("k2", "v2"));
    Assertions.assertEquals(topicIdent.name().toLowerCase(), alteredTopic.name());

    // test case-insensitive in dropping
    Assertions.assertTrue(
        topicNormalizeDispatcher.dropTopic(
            NameIdentifier.of(topicNs, topicIdent.name().toUpperCase())));
    Assertions.assertFalse(topicNormalizeDispatcher.topicExists(topicIdent));
  }

  @Test
  public void testNameSpec() {
    Namespace topicNs = Namespace.of(metalake, catalog, "testNameSpec");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaNormalizeDispatcher.createSchema(NameIdentifier.of(topicNs.levels()), "comment", props);

    NameIdentifier topicIdent = NameIdentifier.of(topicNs, SECURABLE_ENTITY_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> topicNormalizeDispatcher.createTopic(topicIdent, "comment", null, props));
    Assertions.assertEquals(
        "The TOPIC name '*' is reserved. Illegal name: *", exception.getMessage());

    NameIdentifier topicIdent2 = NameIdentifier.of(topicNs, "a?");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> topicNormalizeDispatcher.createTopic(topicIdent2, "comment", null, props));
    Assertions.assertEquals(
        "The TOPIC name 'a?' is illegal. Illegal name: a?", exception.getMessage());
  }
}
