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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicChange;

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
