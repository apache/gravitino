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

package org.apache.gravitino.lineage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import io.openlineage.server.OpenLineage.RunEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.EventWrapper;

public class LineageSinkManager {

  private EventBus eventBus;
  private EventListenerManager eventListenerManager;

  private static final Splitter splitter = Splitter.on(",");

  public LineageSinkManager() {
    this.eventListenerManager = new EventListenerManager();
  }

  public void initialize(Map<String, String> config) {
    Map<String, String> eventListenerConfigs = transformToEventListenerConfigs(config);
    eventListenerManager.init(eventListenerConfigs);
    this.eventBus = eventListenerManager.createEventBus();
    eventListenerManager.start();
  }

  public boolean isHighWaterMark() {
    return false;
  }

  private Map<String, String> transformToEventListenerConfigs(Map<String, String> lineageConfigs) {
    Map<String, String> eventListenerConfigs = new HashMap<>();

    for (Entry<String, String> entry : lineageConfigs.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(LineageConfig.LINEAGE_CONFIG_SINKS)) {
        String sinks = entry.getValue();
        eventListenerConfigs.put(
            EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES, entry.getValue());
        splitter
            .omitEmptyStrings()
            .trimResults()
            .splitToStream(sinks)
            .forEach(
                sinkName -> {
                  Preconditions.checkArgument(
                      lineageConfigs.containsKey(
                          sinkName + "." + LineageConfig.LINEAGE_SINK_CLASS_NAME),
                      "");
                  eventListenerConfigs.put(
                      sinkName + "." + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
                      LineageSinkEventListener.class.getName());
                });
      } else {
        eventListenerConfigs.put(entry.getKey(), entry.getValue());
      }
    }
    return eventListenerConfigs;
  }

  public void sink(RunEvent runEvent) {
    eventBus.dispatchEvent(new EventWrapper(runEvent));
  }

  public void stop() {
    eventListenerManager.stop();
  }

  @VisibleForTesting
  EventBus eventBus() {
    return eventBus;
  }
}
