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
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.gravitino.listener.api.event.SupportsChangingPreEvent;

/**
 * The {@code EventBus} class serves as a mechanism to dispatch events to registered listeners. It
 * supports both synchronous and asynchronous listeners by categorizing them into two distinct types
 * within its internal management.
 */
public class EventBus {
  /**
   * Holds all instances of {@link EventListenerPlugin}. These instances can either be {@link
   * EventListenerPluginWrapper} which are used for synchronous event process, or {@link
   * AsyncQueueListener} for asynchronous event processing.
   */
  private final List<EventListenerPlugin> eventListeners;

  /**
   * Holds instances of {@link AsyncQueueListener}, mainly used to check the status of async queue,
   * like {@link #isHighWatermark()}.
   */
  private final List<AsyncQueueListener> asyncQueueListeners;

  /**
   * Constructs an EventBus with a predefined list of event listeners.
   *
   * @param eventListeners A list of {@link EventListenerPlugin} instances that are to be registered
   *     with this EventBus for event dispatch.
   */
  public EventBus(List<EventListenerPlugin> eventListeners) {
    this.eventListeners = eventListeners;
    this.asyncQueueListeners =
        eventListeners.stream()
            .filter(AsyncQueueListener.class::isInstance)
            .map(AsyncQueueListener.class::cast)
            .collect(Collectors.toList());
  }

  /**
   * Dispatches an event to all registered listeners. Each listener processes the event based on its
   * implementation, which could be either synchronous or asynchronous.
   *
   * @param baseEvent The event to be dispatched to all registered listeners.
   * @return an Optional containing the transformed pre-event if it implements {@link
   *     SupportsChangingPreEvent}, otherwise {@link Optional#empty() empty}
   */
  public Optional<BaseEvent> dispatchEvent(BaseEvent baseEvent) {
    if (baseEvent instanceof PreEvent) {
      return dispatchAndTransformPreEvent((PreEvent) baseEvent);
    } else if (baseEvent instanceof Event) {
      dispatchPostEvent((Event) baseEvent);
      return Optional.empty();
    } else {
      throw new RuntimeException("Unknown event type:" + baseEvent.getClass().getSimpleName());
    }
  }

  public boolean isHighWatermark() {
    return asyncQueueListeners.stream().anyMatch(AsyncQueueListener::isHighWatermark);
  }

  /**
   * Retrieves the list of registered post-event listeners. This method is primarily intended for
   * testing purposes to verify the correct registration and functioning of event listeners.
   *
   * @return A list of {@link EventListenerPlugin} instances currently registered with this
   *     EventBus.
   */
  @VisibleForTesting
  public List<EventListenerPlugin> getEventListeners() {
    return eventListeners;
  }

  private void dispatchPostEvent(Event postEvent) {
    eventListeners.forEach(eventListener -> eventListener.onPostEvent(postEvent));
  }

  private Optional<BaseEvent> dispatchAndTransformPreEvent(PreEvent originalEvent)
      throws ForbiddenException {
    boolean supportsChangePreEvent = originalEvent instanceof SupportsChangingPreEvent;
    PreEvent preEvent;
    if (supportsChangePreEvent) {
      preEvent = (PreEvent) transformPreEvent((SupportsChangingPreEvent) originalEvent);
    } else {
      preEvent = originalEvent;
    }
    eventListeners.forEach(eventListener -> eventListener.onPreEvent(preEvent));
    return supportsChangePreEvent ? Optional.of(preEvent) : Optional.empty();
  }

  private SupportsChangingPreEvent transformPreEvent(SupportsChangingPreEvent preEvent) {
    SupportsChangingPreEvent tmpPreEvent = preEvent;
    for (EventListenerPlugin eventListener : eventListeners) {
      tmpPreEvent = eventListener.transformPreEvent(tmpPreEvent);
      Preconditions.checkNotNull(
          tmpPreEvent,
          String.format("%s transformPreEvent return null", getListenerName(eventListener)));
    }
    return tmpPreEvent;
  }

  private String getListenerName(EventListenerPlugin eventListener) {
    if (eventListener instanceof EventListenerPluginWrapper) {
      return ((EventListenerPluginWrapper) eventListener).listenerName();
    }
    return eventListener.getClass().getSimpleName();
  }
}
