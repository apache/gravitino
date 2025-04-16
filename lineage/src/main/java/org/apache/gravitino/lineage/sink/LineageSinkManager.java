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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.openlineage.server.OpenLineage.RunEvent;
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lineage.LineageConfig;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.EventWrapper;

public class LineageSinkManager implements Closeable {
  private EventBus eventBus;
  private final EventListenerManager eventListenerManager;

  public LineageSinkManager() {
    this.eventListenerManager = new EventListenerManager();
  }

  public void initialize(List<String> sinks, Map<String, String> LineageConfigs) {
    Map<String, String> eventListenerConfigs =
        transformToEventListenerConfigs(sinks, LineageConfigs);
    eventListenerManager.init(eventListenerConfigs);
    this.eventBus = eventListenerManager.createEventBus();
    eventListenerManager.start();
  }

  // Checks if the sink queue size surpasses the threshold to avoid overwhelming lineage sinks.
  public boolean isHighWatermark() {
    return eventBus.isHighWatermark();
  }

  /**
   * The lineage event is dispatched by event listener system with a dedicated async event listener
   * {@link LineageSinkEventListener} which wrap the lineage sink class. Consequently, we must
   * convert the lineage configuration into event listener configurations. This conversion includes
   * details such as event listener names, event listener classes, and the capacity of the
   * asynchronous event queue.
   */
  @VisibleForTesting
  static Map<String, String> transformToEventListenerConfigs(
      List<String> sinks, Map<String, String> lineageConfigs) {
    Map<String, String> eventListenerConfigs = new HashMap<>();
    eventListenerConfigs.putAll(generateEventListenerConfigs(sinks, lineageConfigs));
    eventListenerConfigs.putAll(lineageConfigs);
    return eventListenerConfigs;
  }

  private static Map<String, String> generateEventListenerConfigs(
      List<String> sinks, Map<String, String> lineageConfigs) {
    HashMap eventListenerConfigs = new HashMap();
    if (sinks.isEmpty()) {
      return eventListenerConfigs;
    }

    String queueCapacity = lineageConfigs.get(LineageConfig.LINEAGE_SINK_QUEUE_CAPACITY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(queueCapacity), "Lineage sink queue capacity is not set");
    int capacityPerSink = Integer.valueOf(queueCapacity) / sinks.size();

    eventListenerConfigs.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES, String.join(",", sinks));
    sinks.forEach(
        sinkName -> {
          eventListenerConfigs.put(
              sinkName + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
              LineageSinkEventListener.class.getName());
          eventListenerConfigs.put(
              sinkName + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_QUEUE_CAPACITY,
              String.valueOf(capacityPerSink));
        });
    return eventListenerConfigs;
  }

  public void sink(RunEvent runEvent) {
    eventBus.dispatchEvent(new EventWrapper(runEvent));
  }

  @Override
  public void close() {
    if (eventListenerManager != null) {
      eventListenerManager.stop();
    }
  }

  @VisibleForTesting
  EventBus eventBus() {
    return eventBus;
  }
}
