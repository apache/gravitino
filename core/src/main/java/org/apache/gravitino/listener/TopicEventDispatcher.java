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

package org.apache.gravitino.listener;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.listener.api.event.AlterTopicEvent;
import org.apache.gravitino.listener.api.event.AlterTopicFailureEvent;
import org.apache.gravitino.listener.api.event.CreateTopicEvent;
import org.apache.gravitino.listener.api.event.CreateTopicFailureEvent;
import org.apache.gravitino.listener.api.event.DropTopicEvent;
import org.apache.gravitino.listener.api.event.DropTopicFailureEvent;
import org.apache.gravitino.listener.api.event.ListTopicEvent;
import org.apache.gravitino.listener.api.event.ListTopicFailureEvent;
import org.apache.gravitino.listener.api.event.LoadTopicEvent;
import org.apache.gravitino.listener.api.event.LoadTopicFailureEvent;
import org.apache.gravitino.listener.api.info.TopicInfo;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code TopicEventDispatcher} is a decorator for {@link TopicDispatcher} that not only delegates
 * topic operations to the underlying catalog dispatcher but also dispatches corresponding events to
 * an {@link org.apache.gravitino.listener.EventBus} after each operation is completed. This allows
 * for event-driven workflows or monitoring of topic operations.
 */
public class TopicEventDispatcher implements TopicDispatcher {
  private final EventBus eventBus;
  private final TopicDispatcher dispatcher;

  /**
   * Constructs a TopicEventDispatcher with a specified EventBus and TopicCatalog.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link TopicDispatcher} that will perform the actual topic
   *     operations.
   */
  public TopicEventDispatcher(EventBus eventBus, TopicDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public Topic alterTopic(NameIdentifier ident, TopicChange... changes)
      throws NoSuchTopicException, IllegalArgumentException {
    try {
      Topic topic = dispatcher.alterTopic(ident, changes);
      eventBus.dispatchEvent(
          new AlterTopicEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new TopicInfo(topic)));
      return topic;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterTopicFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropTopic(NameIdentifier ident) {
    try {
      boolean isExists = dispatcher.dropTopic(ident);
      eventBus.dispatchEvent(
          new DropTopicEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropTopicFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public NameIdentifier[] listTopics(Namespace namespace) throws NoSuchTopicException {
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listTopics(namespace);
      eventBus.dispatchEvent(new ListTopicEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListTopicFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public Topic loadTopic(NameIdentifier ident) throws NoSuchTopicException {
    try {
      Topic topic = dispatcher.loadTopic(ident);
      eventBus.dispatchEvent(
          new LoadTopicEvent(PrincipalUtils.getCurrentUserName(), ident, new TopicInfo(topic)));
      return topic;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadTopicFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public boolean topicExists(NameIdentifier ident) {
    return dispatcher.topicExists(ident);
  }

  @Override
  public Topic createTopic(
      NameIdentifier ident, String comment, DataLayout dataLayout, Map<String, String> properties)
      throws NoSuchTopicException, TopicAlreadyExistsException {
    try {
      Topic topic = dispatcher.createTopic(ident, comment, dataLayout, properties);
      eventBus.dispatchEvent(
          new CreateTopicEvent(PrincipalUtils.getCurrentUserName(), ident, new TopicInfo(topic)));
      return topic;
    } catch (Exception e) {
      TopicInfo createTopicRequest = new TopicInfo(ident.name(), comment, properties, null);
      eventBus.dispatchEvent(
          new CreateTopicFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, createTopicRequest));
      throw e;
    }
  }
}
