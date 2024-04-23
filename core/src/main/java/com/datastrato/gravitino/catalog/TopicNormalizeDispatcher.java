/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.messaging.DataLayout;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicChange;
import java.util.Map;

public class TopicNormalizeDispatcher implements TopicDispatcher {

  private final TopicOperationDispatcher dispatcher;

  public TopicNormalizeDispatcher(TopicOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Namespace caseSensitiveNs = applyCaseSensitive(namespace, Capability.Scope.TOPIC, dispatcher);
    NameIdentifier[] identifiers = dispatcher.listTopics(caseSensitiveNs);
    return applyCaseSensitive(identifiers, Capability.Scope.TOPIC, dispatcher);
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.loadTopic(applyCaseSensitive(ident, Capability.Scope.TOPIC, dispatcher));
  }

  @Override
  public boolean topicExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.topicExists(applyCaseSensitive(ident, Capability.Scope.TOPIC, dispatcher));
  }

  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException {
    return dispatcher.createTopic(normalizeNameIdentifier(ident), comment, dataLayout, properties);
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.alterTopic(
        applyCaseSensitive(ident, Capability.Scope.TOPIC, dispatcher), changes);
  }

  @Override
  public boolean dropTopic(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.dropTopic(applyCaseSensitive(ident, Capability.Scope.TOPIC, dispatcher));
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.TOPIC, capability);
  }
}
