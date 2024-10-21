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

package org.apache.gravitino.listener.api.event;

import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.TopicEventDispatcher;
import org.apache.gravitino.listener.api.info.TopicInfo;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestTopicEvent {
  private TopicEventDispatcher dispatcher;
  private TopicEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Topic topic;

  @BeforeAll
  void init() {
    this.topic = mockTopic();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    TopicDispatcher topicDispatcher = mockTopicDispatcher();
    this.dispatcher = new TopicEventDispatcher(eventBus, topicDispatcher);
    TopicDispatcher topicExceptionDispatcher = mockExceptionTopicDispatcher();
    this.failureDispatcher = new TopicEventDispatcher(eventBus, topicExceptionDispatcher);
  }

  @Test
  void testCreateTopicEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "topic");
    dispatcher.createTopic(identifier, topic.comment(), null, topic.properties());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateTopicEvent.class, event.getClass());
    TopicInfo topicInfo = ((CreateTopicEvent) event).createdTopicInfo();
    checkTopicInfo(topicInfo, topic);
  }

  @Test
  void testLoadTopicEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "topic");
    dispatcher.loadTopic(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadTopicEvent.class, event.getClass());
    TopicInfo topicInfo = ((LoadTopicEvent) event).loadedTopicInfo();
    checkTopicInfo(topicInfo, topic);
  }

  @Test
  void testAlterTopicEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "topic");
    TopicChange topicChange = TopicChange.setProperty("a", "b");
    dispatcher.alterTopic(identifier, topicChange);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterTopicEvent.class, event.getClass());
    TopicInfo topicInfo = ((AlterTopicEvent) event).updatedTopicInfo();
    checkTopicInfo(topicInfo, topic);
    Assertions.assertEquals(1, ((AlterTopicEvent) event).topicChanges().length);
    Assertions.assertEquals(topicChange, ((AlterTopicEvent) event).topicChanges()[0]);
  }

  @Test
  void testDropTopicEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "topic");
    dispatcher.dropTopic(identifier);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropTopicEvent.class, event.getClass());
    Assertions.assertEquals(true, ((DropTopicEvent) event).isExists());
  }

  @Test
  void testListTopicEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    dispatcher.listTopics(namespace);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListTopicEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListTopicEvent) event).namespace());
  }

  @Test
  void testCreateTopicFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "topic");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.createTopic(identifier, topic.comment(), null, topic.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateTopicFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreateTopicFailureEvent) event).exception().getClass());
    checkTopicInfo(((CreateTopicFailureEvent) event).createTopicRequest(), topic);
  }

  @Test
  void testLoadTopicFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "topic");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadTopic(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadTopicFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadTopicFailureEvent) event).exception().getClass());
  }

  @Test
  void testAlterTopicFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "topic");
    TopicChange topicChange = TopicChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterTopic(identifier, topicChange));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterTopicFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterTopicFailureEvent) event).exception().getClass());
    Assertions.assertEquals(1, ((AlterTopicFailureEvent) event).topicChanges().length);
    Assertions.assertEquals(topicChange, ((AlterTopicFailureEvent) event).topicChanges()[0]);
  }

  @Test
  void testDropTopicFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", "topic");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropTopic(identifier));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropTopicFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropTopicFailureEvent) event).exception().getClass());
  }

  @Test
  void testListTopicFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listTopics(namespace));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListTopicFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListTopicFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListTopicFailureEvent) event).namespace());
  }

  private void checkTopicInfo(TopicInfo topicInfo, Topic topic) {
    Assertions.assertEquals(topic.name(), topicInfo.name());
    Assertions.assertEquals(topic.properties(), topicInfo.properties());
    Assertions.assertEquals(topic.comment(), topicInfo.comment());
  }

  private Topic mockTopic() {
    Topic topic = mock(Topic.class);
    when(topic.comment()).thenReturn("comment");
    when(topic.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(topic.name()).thenReturn("topic");
    when(topic.auditInfo()).thenReturn(null);
    return topic;
  }

  private TopicDispatcher mockTopicDispatcher() {
    TopicDispatcher dispatcher = mock(TopicDispatcher.class);
    when(dispatcher.createTopic(
            any(NameIdentifier.class), any(String.class), isNull(), any(Map.class)))
        .thenReturn(topic);
    when(dispatcher.loadTopic(any(NameIdentifier.class))).thenReturn(topic);
    when(dispatcher.dropTopic(any(NameIdentifier.class))).thenReturn(true);
    when(dispatcher.listTopics(any(Namespace.class))).thenReturn(null);
    when(dispatcher.alterTopic(any(NameIdentifier.class), any(TopicChange.class)))
        .thenReturn(topic);
    return dispatcher;
  }

  private TopicDispatcher mockExceptionTopicDispatcher() {
    TopicDispatcher dispatcher =
        mock(
            TopicDispatcher.class,
            invocation -> {
              throw new GravitinoRuntimeException("Exception for all methods");
            });
    return dispatcher;
  }
}
