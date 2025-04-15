/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lineage.sink;

import io.openlineage.server.OpenLineage.RunEvent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.gravitino.lineage.LineageConfig;
import org.apache.gravitino.listener.AsyncQueueListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.EventListenerPluginWrapper;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestLineageSinkManager {

  @SneakyThrows
  @Test
  void testLineageSinkManager() {
    LineageSinkManager lineageSinkManager = new LineageSinkManager();
    lineageSinkManager.initialize(Arrays.asList("sink1", "sink2"), getLineageSinkConfig());
    lineageSinkManager.sink(getEvent());
    EventBus eventBus = lineageSinkManager.eventBus();
    List<EventListenerPlugin> listeners = eventBus.getEventListeners();
    Assertions.assertEquals(2, listeners.size());

    listeners.stream()
        .forEach(
            listener -> {
              Assertions.assertTrue(listener instanceof AsyncQueueListener);
              AsyncQueueListener asyncQueueListener = (AsyncQueueListener) listener;
              List<EventListenerPlugin> internalListeners = asyncQueueListener.getEventListeners();
              Assertions.assertEquals(1, internalListeners.size());
              Assertions.assertTrue(internalListeners.get(0) instanceof EventListenerPluginWrapper);
              EventListenerPlugin userListener =
                  ((EventListenerPluginWrapper) internalListeners.get(0)).getUserEventListener();
              Assertions.assertTrue(userListener instanceof LineageSinkEventListener);
              LineageSink sink = ((LineageSinkEventListener) userListener).lineageSink();
              Assertions.assertTrue(sink instanceof LineageSinkForTest);
              checkLineageSink((LineageSinkForTest) sink);
            });
  }

  @Test
  void testTransformToEventListenerConfigs() {
    Map<String, String> configs =
        LineageSinkManager.transformToEventListenerConfigs(
            Arrays.asList("sink1", "sink2"), getLineageSinkConfig());

    Assertions.assertEquals(
        "sink1,sink2", configs.get(EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES));

    Assertions.assertEquals(
        "500",
        configs.get("sink1." + EventListenerManager.GRAVITINO_EVENT_LISTENER_QUEUE_CAPACITY));
    Assertions.assertEquals(
        LineageSinkEventListener.class.getName(),
        configs.get("sink1." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS));
    Assertions.assertEquals("sink1", configs.get("sink1.name"));
    Assertions.assertEquals("a", configs.get("sink1.a"));

    Assertions.assertEquals(
        "500",
        configs.get("sink2." + EventListenerManager.GRAVITINO_EVENT_LISTENER_QUEUE_CAPACITY));
    Assertions.assertEquals(
        LineageSinkEventListener.class.getName(),
        configs.get("sink2." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS));
    Assertions.assertEquals("b", configs.get("sink2.a"));
  }

  private void checkLineageSink(LineageSinkForTest sink) {
    Map<String, String> configs = sink.getConfigs();
    Assertions.assertTrue(configs.containsKey("name"));

    String name = configs.get("name");
    Assertions.assertTrue("sink1".equals(name) || "sink2".equals(name));
    if ("sink1".equals(name)) {
      Assertions.assertEquals("a", configs.get("a"));
    } else if ("sink2".equals(name)) {
      Assertions.assertEquals("b", configs.get("a"));
    }

    List<RunEvent> events = sink.tryGetEvents();
    Assertions.assertEquals(1, events.size());
  }

  private RunEvent getEvent() {
    return Mockito.mock(RunEvent.class);
  }

  private Map<String, String> getLineageSinkConfig() {
    Map<String, String> lineageSinkConfigs = new HashMap<>();
    lineageSinkConfigs.put(
        "sink1." + LineageConfig.LINEAGE_SINK_CLASS_NAME, LineageSinkForTest.class.getName());
    lineageSinkConfigs.put("sink1.a", "a");
    lineageSinkConfigs.put("sink1.name", "sink1");
    lineageSinkConfigs.put(
        "sink2." + LineageConfig.LINEAGE_SINK_CLASS_NAME, LineageSinkForTest.class.getName());
    lineageSinkConfigs.put("sink2.a", "b");
    lineageSinkConfigs.put("sink2.name", "sink2");
    lineageSinkConfigs.put(LineageConfig.LINEAGE_SINK_QUEUE_CAPACITY, "1000");

    return lineageSinkConfigs;
  }
}
