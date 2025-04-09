package org.apache.gravitino.listener.api;

import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;

/** Interface for dispatching events to the listener. */
public interface EventDispatcher {

  /**
   * Dispatches a {@link PreEvent} to the listener.
   *
   * @param event the {@link PreEvent} to dispatch.
   * @throws ForbiddenException if plugin is not allowed to dispatch the event.
   */
  void dispatchPreEvent(PreEvent event) throws ForbiddenException;

  /**
   * Dispatches an {@link Event} to the listener.
   *
   * @param event the {@link Event} to dispatch.
   */
  void dispatchPostEvent(Event event);
}
