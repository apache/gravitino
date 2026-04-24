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
package org.apache.gravitino.hook;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code TopicHookDispatcher} is a decorator for {@link TopicDispatcher} that not only delegates
 * topic operations to the underlying topic dispatcher but also executes some hook operations before
 * or after the underlying operations.
 */
public class TopicHookDispatcher implements TopicDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(TopicHookDispatcher.class);
  private final TopicDispatcher dispatcher;

  public TopicHookDispatcher(TopicDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listTopics(namespace);
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    return dispatcher.loadTopic(ident);
  }

  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchSchemaException, TopicAlreadyExistsException {
    Topic topic = dispatcher.createTopic(ident, comment, dataLayout, properties);

    // Set the creator as the owner of the topic.
    try {
      OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
      if (ownerManager != null) {
        // The inner NormalizeDispatcher case-folds the topic name (and its schema namespace)
        // based on catalog capabilities, so the entity is stored under the normalized identifier.
        // Apply the same normalization here so the owner is attached to the same identifier the
        // manager sees.
        NameIdentifier normalizedIdent =
            CapabilityHelpers.applyCapabilities(
                ident, Capability.Scope.TOPIC, GravitinoEnv.getInstance().catalogManager());
        ownerManager.setOwner(
            normalizedIdent.namespace().level(0),
            NameIdentifierUtil.toMetadataObject(normalizedIdent, Entity.EntityType.TOPIC),
            PrincipalUtils.getCurrentUserName(),
            Owner.Type.USER);
      }
    } catch (Exception e) {
      LOG.warn("Failed to set owner for topic {}, topic exists without owner", ident, e);
    }
    return topic;
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    return dispatcher.alterTopic(ident, changes);
  }

  @Override
  public boolean dropTopic(NameIdentifier ident) {
    List<String> locations =
        AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.TOPIC);
    boolean dropped = dispatcher.dropTopic(ident);
    AuthorizationUtils.authorizationPluginRemovePrivileges(
        ident, Entity.EntityType.TOPIC, locations);
    return dropped;
  }

  @Override
  public boolean topicExists(NameIdentifier ident) {
    return dispatcher.topicExists(ident);
  }
}
