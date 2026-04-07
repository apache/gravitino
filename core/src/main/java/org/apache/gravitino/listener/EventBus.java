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
import org.apache.gravitino.listener.api.event.FailureEvent;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.gravitino.listener.api.event.SupportsChangingPreEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code EventBus} class serves as a mechanism to dispatch events to registered listeners. It
 * supports both synchronous and asynchronous listeners by categorizing them into two distinct types
 * within its internal management.
 */
public class EventBus {
  private static final Logger LOG = LoggerFactory.getLogger(EventBus.class);

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
   * <p>This method applies different exception-handling semantics depending on the event type:
   *
   * <ul>
   *   <li>{@link PreEvent}: May throw {@link ForbiddenException} to prevent the operation (e.g.,
   *       for authorization or validation failures).
   *   <li>{@link FailureEvent}: All listener exceptions are caught, logged, and swallowed to avoid
   *       masking the original failure.
   *   <li>{@link Event} (success events): Listener exceptions are propagated to the caller.
   * </ul>
   *
   * @param baseEvent the event to dispatch to all registered listeners; must not be null
   * @return an {@link Optional} containing the transformed pre-event if it implements {@link
   *     SupportsChangingPreEvent}, otherwise {@link Optional#empty()}
   * @throws ForbiddenException if a synchronous pre-event listener blocks the operation
   * @throws RuntimeException if an unknown event type is encountered or a listener throws an
   *     exception for non-failure events
   */
  public Optional<BaseEvent> dispatchEvent(BaseEvent baseEvent) {
    Preconditions.checkNotNull(baseEvent, "baseEvent cannot be null");
    if (baseEvent instanceof PreEvent) {
      return dispatchAndTransformPreEvent((PreEvent) baseEvent);
    } else if (baseEvent instanceof FailureEvent) {
      // FailureEvents get special "safe" handling - swallow all exceptions to prevent
      // masking the original error that triggered this failure event
      dispatchFailureEvent((Event) baseEvent);
      return Optional.empty();
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

  /**
   * Dispatches a failure failureEvent to listeners, swallowing all exceptions to preserve the
   * original error.
   *
   * <p>When the primary operation fails, we want to notify listeners, but listener exceptions must
   * NOT propagate and mask the original failure. This method catches and logs all exceptions
   * (including {@link RuntimeException}, {@link Error}, etc.) to ensure the original exception can
   * be properly thrown to the caller.
   *
   * @param failureEvent the failure failureEvent to dispatch
   */
  private void dispatchFailureEvent(Event failureEvent) {
    try {
      eventListeners.forEach(eventListener -> eventListener.onPostEvent(failureEvent));
    } catch (Exception e) {
      // Swallow ALL listener exceptions to prevent masking the original error
      LOG.error(
          "Failed to dispatch failure failureEvent: {}, ignoring exception to preserve original error",
          failureEvent.getClass().getSimpleName(),
          e);
    }
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
      Preconditions.checkArgument(
          tmpPreEvent != null,
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
