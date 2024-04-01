/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.impl;

import com.datastrato.gravitino.listener.EventListenerPlugin;
import com.datastrato.gravitino.listener.event.Event;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for user provided event listener, could contain common logic like exception handling,
 * recording metrics, recording slow event process.
 */
public class EventListenerPluginWrapper implements EventListenerPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(EventListenerPluginWrapper.class);
  private String listenerName;
  private EventListenerPlugin userEventListener;

  public EventListenerPluginWrapper(String listenerName, EventListenerPlugin userEventListener) {
    this.listenerName = listenerName;
    this.userEventListener = userEventListener;
  }

  @Override
  public void init(Map<String, String> properties) {
    throw new RuntimeException(
        "Should not reach here, the event listener has already been initialized.");
  }

  @Override
  public void start() {
    userEventListener.start();
    LOG.info("Start event listener {}.", listenerName);
  }

  @Override
  public void stop() {
    try {
      userEventListener.stop();
      LOG.info("Stop event listener {}.", listenerName);
    } catch (Exception e) {
      LOG.warn("Failed to stop event listener {}.", listenerName, e);
    }
  }

  @Override
  public void onPostEvent(Event event) {
    try {
      userEventListener.onPostEvent(event);
    } catch (Exception e) {
      LOG.warn(
          "Event listener {} process event {} failed,",
          listenerName,
          event.getClass().getSimpleName(),
          e);
    }
  }

  @VisibleForTesting
  EventListenerPlugin getUserEventListener() {
    return userEventListener;
  }
}
