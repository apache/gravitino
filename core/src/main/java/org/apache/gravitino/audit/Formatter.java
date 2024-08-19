package org.apache.gravitino.audit;

import org.apache.gravitino.listener.api.event.Event;

/**
 * Interface for formatting the event.
 *
 * @param <R>
 */
public interface Formatter<R> {

  /**
   * Format the event.
   *
   * @param event The event to format.
   * @return The formatted event.
   */
  R format(Event event);
}
