/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.impl;

import com.datastrato.gravitino.listener.EventListenerPlugin;
import com.datastrato.gravitino.listener.event.Event;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;

/**
 * The {@code EventBus} class serves as a mechanism to dispatch events to registered listeners. It
 * supports both synchronous and asynchronous listeners by categorizing them into two distinct types
 * within its internal management.
 */
public class EventBus {
  // Holds instances of EventListenerPlugin. These instances can either be
  // EventListenerPluginWrapper,
  // which are meant for synchronous event listening, or AsyncQueueListener, designed for
  // asynchronous event processing.
  private final List<EventListenerPlugin> postEventListeners;

  /**
   * Constructs an EventBus with a predefined list of event listeners.
   *
   * @param postEventListeners A list of {@link EventListenerPlugin} instances that are to be
   *     registered with this EventBus for event dispatch.
   */
  public EventBus(List<EventListenerPlugin> postEventListeners) {
    this.postEventListeners = postEventListeners;
  }

  /**
   * Dispatches an event to all registered listeners. Each listener processes the event based on its
   * implementation, which could be either synchronously or asynchronously.
   *
   * @param event The event to be dispatched to all registered listeners.
   */
  public void dispatchEvent(Event event) {
    postEventListeners.forEach(postEventListener -> postEventListener.onPostEvent(event));
  }

  /**
   * Retrieves the list of registered post-event listeners. This method is primarily intended for
   * testing purposes to verify the correct registration and functioning of event listeners.
   *
   * @return A list of {@link EventListenerPlugin} instances currently registered with this
   *     EventBus.
   */
  @VisibleForTesting
  List<EventListenerPlugin> getPostEventListeners() {
    return postEventListeners;
  }
}
