/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

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

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;

public class TopicStandardizedDispatcher implements TopicDispatcher {

  private final TopicOperationDispatcher dispatcher;

  public TopicStandardizedDispatcher(TopicOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    Capability capability = dispatcher.getCatalogCapability(namespace);
    Namespace standardizedNamespace = applyCapabilities(namespace, Capability.Scope.TOPIC, capability);
    NameIdentifier[] identifiers = dispatcher.listTopics(standardizedNamespace);
    return applyCapabilities(identifiers, Capability.Scope.TOPIC, capability);
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    return dispatcher.loadTopic(standardizeNameIdentifier(ident));
  }

  @Override
  public boolean topicExists(NameIdentifier ident) {
    return dispatcher.topicExists(standardizeNameIdentifier(ident));
  }

  @Override
  public Topic createTopic(NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties) throws NoSuchSchemaException, TopicAlreadyExistsException {
    return dispatcher.createTopic(standardizeNameIdentifier(ident), comment, dataLayout, properties);
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes) throws NoSuchTopicException, IllegalArgumentException {
    return dispatcher.alterTopic(standardizeNameIdentifier(ident), changes);
  }

  @Override
  public boolean dropTopic(NameIdentifier ident) {
    return dispatcher.dropTopic(standardizeNameIdentifier(ident));
  }

  private NameIdentifier standardizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.TOPIC, capability);
  }
}
