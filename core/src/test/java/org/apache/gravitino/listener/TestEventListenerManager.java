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

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.DummyEventListener.DummyAsyncEventListener;
import org.apache.gravitino.listener.DummyEventListener.DummyAsyncIsolatedEventListener;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEventListenerManager {
  static class DummyEvent extends Event {
    protected DummyEvent(String user, NameIdentifier identifier) {
      super(user, identifier);
    }
  }

  private static final DummyEvent DUMMY_EVENT_INSTANCE =
      new DummyEvent("user", NameIdentifier.of("a", "b"));

  @Test
  void testSyncListener() {
    String sync1 = "sync1";
    String sync2 = "sync2";
    Map<String, String> properties = createSyncEventListenerConfig(sync1, sync2);

    EventListenerManager eventListenerManager = new EventListenerManager();
    eventListenerManager.init(properties);
    eventListenerManager.start();

    EventBus eventBus = eventListenerManager.createEventBus();
    eventBus.dispatchEvent(DUMMY_EVENT_INSTANCE);

    List<EventListenerPlugin> listeners = eventBus.getPostEventListeners();
    Assertions.assertEquals(2, listeners.size());
    Set<String> names =
        listeners.stream()
            .map(
                listener -> {
                  Assertions.assertTrue(listener instanceof EventListenerPluginWrapper);
                  EventListenerPluginWrapper wrapper = (EventListenerPluginWrapper) listener;
                  EventListenerPlugin userListener = wrapper.getUserEventListener();
                  Assertions.assertTrue(userListener instanceof DummyEventListener);
                  checkEvents(((DummyEventListener) userListener).getEvents());
                  return ((DummyEventListener) userListener).properties.get("name");
                })
            .collect(Collectors.toSet());
    Assertions.assertEquals(ImmutableSet.of(sync1, sync2), names);

    eventListenerManager.stop();
  }

  @Test
  void testSharedAsyncListeners() {
    String async1 = "async1";
    String async2 = "async2";
    Map<String, String> properties = createAsyncEventListenerConfig(async1, async2);

    EventListenerManager eventListenerManager = new EventListenerManager();
    eventListenerManager.init(properties);
    eventListenerManager.start();

    EventBus eventBus = eventListenerManager.createEventBus();
    eventBus.dispatchEvent(DUMMY_EVENT_INSTANCE);
    List<EventListenerPlugin> listeners = eventBus.getPostEventListeners();

    Assertions.assertEquals(1, listeners.size());
    Assertions.assertTrue(listeners.get(0) instanceof AsyncQueueListener);
    AsyncQueueListener asyncQueueListener = (AsyncQueueListener) listeners.get(0);
    List<EventListenerPlugin> shareQueueListeners = asyncQueueListener.getEventListeners();
    Assertions.assertEquals(2, shareQueueListeners.size());
    Set<String> sharedQueueListenerNames =
        shareQueueListeners.stream()
            .map(
                shareQueueListener -> {
                  Assertions.assertTrue(shareQueueListener instanceof EventListenerPluginWrapper);
                  EventListenerPlugin userListener =
                      ((EventListenerPluginWrapper) shareQueueListener).getUserEventListener();
                  Assertions.assertTrue(userListener instanceof DummyAsyncEventListener);
                  checkEvents(((DummyAsyncEventListener) userListener).tryGetEvents());
                  return ((DummyAsyncEventListener) userListener).properties.get("name");
                })
            .collect(Collectors.toSet());
    Assertions.assertEquals(ImmutableSet.of(async1, async2), sharedQueueListenerNames);

    eventListenerManager.stop();
  }

  @Test
  void testIsolatedAsyncListeners() {
    String async1 = "async1";
    String async2 = "async2";
    Map<String, String> properties = createIsolatedAsyncEventListenerConfig(async1, async2);

    EventListenerManager eventListenerManager = new EventListenerManager();
    eventListenerManager.init(properties);
    eventListenerManager.start();

    EventBus eventBus = eventListenerManager.createEventBus();
    eventBus.dispatchEvent(DUMMY_EVENT_INSTANCE);
    List<EventListenerPlugin> listeners = eventBus.getPostEventListeners();

    Assertions.assertEquals(2, listeners.size());
    Set<String> isolatedListenerNames =
        listeners.stream()
            .map(
                listener -> {
                  Assertions.assertTrue(listener instanceof AsyncQueueListener);
                  AsyncQueueListener asyncQueueListener = (AsyncQueueListener) listener;
                  List<EventListenerPlugin> internalListeners =
                      asyncQueueListener.getEventListeners();
                  Assertions.assertEquals(1, internalListeners.size());
                  Assertions.assertTrue(
                      internalListeners.get(0) instanceof EventListenerPluginWrapper);
                  EventListenerPlugin userListener =
                      ((EventListenerPluginWrapper) internalListeners.get(0))
                          .getUserEventListener();
                  Assertions.assertTrue(userListener instanceof DummyAsyncEventListener);
                  checkEvents(((DummyAsyncEventListener) userListener).tryGetEvents());
                  return ((DummyAsyncEventListener) userListener).properties.get("name");
                })
            .collect(Collectors.toSet());
    Assertions.assertEquals(ImmutableSet.of(async1, async2), isolatedListenerNames);

    eventListenerManager.stop();
  }

  private Map<String, String> createIsolatedAsyncEventListenerConfig(String async1, String async2) {
    Map<String, String> config = new HashMap<>();

    config.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES, String.join(",", async1, async2));

    config.put(
        async1 + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        DummyAsyncIsolatedEventListener.class.getName());
    config.put(async1 + ".name", async1);

    config.put(
        async2 + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        DummyAsyncIsolatedEventListener.class.getName());
    config.put(async2 + ".name", async2);

    return config;
  }

  private Map<String, String> createAsyncEventListenerConfig(String async1, String async2) {
    Map<String, String> config = new HashMap<>();

    config.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES, String.join(",", async1, async2));

    config.put(
        async1 + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        DummyAsyncEventListener.class.getName());
    config.put(async1 + ".name", async1);

    config.put(
        async2 + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        DummyAsyncEventListener.class.getName());
    config.put(async2 + ".name", async2);

    return config;
  }

  private Map<String, String> createSyncEventListenerConfig(String sync1, String sync2) {
    Map<String, String> config = new HashMap<>();

    config.put(EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES, String.join(",", sync1, sync2));

    config.put(
        sync1 + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        DummyEventListener.class.getName());
    config.put(sync1 + ".name", sync1);

    config.put(
        sync2 + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        DummyEventListener.class.getName());
    config.put(sync2 + ".name", sync2);

    return config;
  }

  private void checkEvents(List<Event> events) {
    Assertions.assertEquals(1, events.size());
    Assertions.assertEquals(DUMMY_EVENT_INSTANCE, events.get(0));
  }
}
