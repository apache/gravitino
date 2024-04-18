/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

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

public class TestTopicStandardizedDispatcher extends TestTopicOperationDispatcher {
  private static TopicStandardizedDispatcher topicStandardizedDispatcher;
  private static SchemaStandardizedDispatcher schemaStandardizedDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    TestTopicOperationDispatcher.initialize();
    schemaStandardizedDispatcher = new SchemaStandardizedDispatcher(schemaOperationDispatcher);
    topicStandardizedDispatcher = new TopicStandardizedDispatcher(topicOperationDispatcher);
  }

  @Test
  public void testNameCaseInsensitive() {
    Namespace topicNs = Namespace.of(metalake, catalog, "schema161");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaStandardizedDispatcher.createSchema(
        NameIdentifier.of(topicNs.levels()), "comment", props);

    // test case-insensitive in creation
    NameIdentifier topicIdent = NameIdentifier.of(topicNs, "topicNAME");
    Topic createdTopic =
        topicStandardizedDispatcher.createTopic(topicIdent, "comment", null, props);
    Assertions.assertEquals(topicIdent.name().toLowerCase(), createdTopic.name());

    // test case-insensitive in loading
    Topic loadedTopic = topicStandardizedDispatcher.loadTopic(topicIdent);
    Assertions.assertEquals(topicIdent.name().toLowerCase(), loadedTopic.name());

    // test case-insensitive in listing
    NameIdentifier[] idents = topicStandardizedDispatcher.listTopics(topicNs);
    Assertions.assertEquals(1, idents.length);

    // test case-insensitive in altering
    Topic alteredTopic =
        topicStandardizedDispatcher.alterTopic(
            NameIdentifier.of(topicNs, topicIdent.name().toLowerCase()),
            TopicChange.setProperty("k2", "v2"));
    Assertions.assertEquals(topicIdent.name().toLowerCase(), alteredTopic.name());

    // test case-insensitive in dropping
    Assertions.assertTrue(
        topicStandardizedDispatcher.dropTopic(
            NameIdentifier.of(topicNs, topicIdent.name().toUpperCase())));
    Assertions.assertFalse(topicStandardizedDispatcher.topicExists(topicIdent));
  }
}
