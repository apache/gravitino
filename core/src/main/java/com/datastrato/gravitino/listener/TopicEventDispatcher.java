/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.TopicDispatcher;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.listener.api.event.AlterTopicEvent;
import com.datastrato.gravitino.listener.api.event.AlterTopicFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateTopicEvent;
import com.datastrato.gravitino.listener.api.event.CreateTopicFailureEvent;
import com.datastrato.gravitino.listener.api.event.DropTopicEvent;
import com.datastrato.gravitino.listener.api.event.DropTopicFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListTopicEvent;
import com.datastrato.gravitino.listener.api.event.ListTopicFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadTopicEvent;
import com.datastrato.gravitino.listener.api.event.LoadTopicFailureEvent;
import com.datastrato.gravitino.listener.api.info.TopicInfo;
import com.datastrato.gravitino.messaging.DataLayout;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.util.Map;

/**
 * {@code TopicEventDispatcher} is a decorator for {@link TopicDispatcher} that not only delegates
 * topic operations to the underlying catalog dispatcher but also dispatches corresponding events to
 * an {@link com.datastrato.gravitino.listener.EventBus} after each operation is completed. This
 * allows for event-driven workflows or monitoring of topic operations.
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
