/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener;

import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * An interface for event listeners that handle events asynchronously. Implementing this interface
 * allows an event listener to choose between processing events with a shared dispatcher or a
 * separate, dedicated dispatcher.
 *
 * <p>Choosing a shared dispatcher contributes to efficient resource utilization by consolidating
 * multiple listeners' events into a single processing queue. In contrast, a separate dispatcher
 * provides an independent processing environment which can be tailored to specific needs, but it
 * may increase the resource footprint due to the additional infrastructure required.
 *
 * <p>Implementors should weigh the benefits of resource conservation against the need for
 * independent event processing when selecting a dispatcher mode.
 *
 * @see Mode for the enumeration of dispatcher modes.
 */
@DeveloperApi
public interface SupportsAsync {

  /**
   * The enumeration defining the two modes of asynchronous event processing:
   *
   * <ul>
   *   <li>{@code ISOLATED} - The event listener has its own unique event-processing queue, allowing
   *       for customization and isolated processing of events. This mode may lead to higher
   *       resource usage due to the need for a dedicated dispatcher.
   *   <li>{@code SHARED} - The event listener uses a common queue with other listeners for event
   *       processing, promoting better resource efficiency by sharing the dispatcher.
   * </ul>
   */
  enum Mode {
    ISOLATED,
    SHARED
  }

  /**
   * Specifies the dispatcher mode used by the event listener for processing events asynchronously.
   * The mode determines whether the listener uses a shared dispatcher with other listeners or an
   * isolated dispatcher dedicated to itself.
   *
   * <p>By default, the event listener is configured to use a shared dispatcher, which is suitable
   * for most scenarios that do not require dedicated processing.
   *
   * @return The {@link Mode} indicating the type of dispatcher used by the event listener.
   */
  default Mode asyncMode() {
    return Mode.SHARED;
  }
}
