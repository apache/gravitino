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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EventListenerManager loads listeners according to the configurations, and assemble the listeners
 * with following rules:
 *
 * <p>Wrap all listener with EventListenerWrapper to do some common process, like exception handing,
 * record metrics.
 *
 * <p>For async listeners with the shared dispatcher, will create a default AsyncQueueListener to
 * assemble the corresponding EventListenerWrappers.
 *
 * <p>For async listeners with the isolated dispatcher, will create a separate AsyncQueueListener
 * for each EventListenerWrapper.
 */
public class EventListenerManager {
  private static final Logger LOG = LoggerFactory.getLogger(EventListenerManager.class);
  public static final String GRAVITINO_EVENT_LISTENER_PREFIX = "gravitino.eventListener.";
  static final String GRAVITINO_EVENT_LISTENER_NAMES = "names";
  @VisibleForTesting static final String GRAVITINO_EVENT_LISTENER_CLASS = "class";
  static final String GRAVITINO_EVENT_LISTENER_QUEUE_CAPACITY = "queueCapacity";
  static final String GRAVITINO_EVENT_LISTENER_DISPATCHER_JOIN_SECONDS = "dispatcherJoinSeconds";
  private static final Splitter splitter = Splitter.on(",");
  private static final Joiner DOT = Joiner.on(".");

  private int queueCapacity;
  private int dispatcherJoinSeconds;
  private List<EventListenerPlugin> eventListeners;

  public void init(Map<String, String> properties) {
    EventListenerConfig config = new EventListenerConfig(properties);
    this.queueCapacity = config.get(EventListenerConfig.QUEUE_CAPACITY);
    this.dispatcherJoinSeconds = config.get(EventListenerConfig.DISPATCHER_JOIN_SECONDS);

    String eventListenerNames = config.get(EventListenerConfig.LISTENER_NAMES);
    Map<String, EventListenerPlugin> userEventListenerPlugins =
        splitter
            .omitEmptyStrings()
            .trimResults()
            .splitToStream(eventListenerNames)
            .collect(
                Collectors.toMap(
                    listenerName -> listenerName,
                    listenerName ->
                        loadUserEventListenerPlugin(
                            listenerName,
                            MapUtils.getPrefixMap(properties, DOT.join(listenerName, ""))),
                    (existingValue, newValue) -> {
                      throw new IllegalStateException(
                          "Duplicate event listener name detected: " + existingValue);
                    }));
    this.eventListeners = assembleEventListeners(userEventListenerPlugins);
  }

  public void start() {
    eventListeners.stream().forEach(listener -> listener.start());
  }

  public void stop() {
    eventListeners.stream().forEach(listener -> listener.stop());
  }

  public EventBus createEventBus() {
    return new EventBus(eventListeners);
  }

  public void addEventListener(String listenerName, EventListenerPlugin listener) {
    eventListeners.add(new EventListenerPluginWrapper(listenerName, listener));
  }

  private List<EventListenerPlugin> assembleEventListeners(
      Map<String, EventListenerPlugin> userEventListeners) {
    List<EventListenerPlugin> sharedQueueListeners = new ArrayList<>();

    List<EventListenerPlugin> listeners =
        userEventListeners.entrySet().stream()
            .map(
                entrySet -> {
                  String listenerName = entrySet.getKey();
                  EventListenerPlugin listener = entrySet.getValue();
                  switch (listener.mode()) {
                    case SYNC:
                      return new EventListenerPluginWrapper(listenerName, listener);
                    case ASYNC_ISOLATED:
                      return new AsyncQueueListener(
                          ImmutableList.of(new EventListenerPluginWrapper(listenerName, listener)),
                          listenerName,
                          queueCapacity,
                          dispatcherJoinSeconds);
                    case ASYNC_SHARED:
                      sharedQueueListeners.add(
                          new EventListenerPluginWrapper(listenerName, listener));
                      return null;
                    default:
                      throw new RuntimeException("Unexpected listener mode:" + listener.mode());
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!sharedQueueListeners.isEmpty()) {
      listeners.add(
          new AsyncQueueListener(
              sharedQueueListeners, "default", queueCapacity, dispatcherJoinSeconds));
    }
    return listeners;
  }

  private EventListenerPlugin loadUserEventListenerPlugin(
      String listenerName, Map<String, String> config) {
    LOG.info("EventListener:{}, config:{}.", listenerName, config);
    String className = config.get(GRAVITINO_EVENT_LISTENER_CLASS);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(className),
        String.format(
            "EventListener:%s, %s%s.%s is not set in configuration.",
            listenerName,
            GRAVITINO_EVENT_LISTENER_PREFIX,
            listenerName,
            GRAVITINO_EVENT_LISTENER_CLASS));

    try {
      EventListenerPlugin listenerPlugin =
          (EventListenerPlugin) Class.forName(className).getDeclaredConstructor().newInstance();
      listenerPlugin.init(config);
      return listenerPlugin;
    } catch (Exception e) {
      LOG.error(
          "Failed to create and initialize event listener {}, className: {}.",
          listenerName,
          className,
          e);
      throw new RuntimeException(e);
    }
  }
}
