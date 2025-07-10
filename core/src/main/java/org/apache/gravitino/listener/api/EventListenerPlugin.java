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

package org.apache.gravitino.listener.api;

import java.util.Map;
import javax.validation.constraints.NotNull;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.gravitino.listener.api.event.SupportsChangingPreEvent;

/**
 * Defines an interface for event listeners that manage the lifecycle and state of a plugin,
 * including its initialization, starting, and stopping processes, as well as the handling of events
 * occurring after various operations have been executed.
 *
 * <p>This interface is intended for developers who implement plugins within a system, providing a
 * structured approach to managing plugin operations and event processing.
 */
@DeveloperApi
public interface EventListenerPlugin {
  /**
   * Defines the operational modes for event processing within an event listener, catering to both
   * synchronous and asynchronous processing strategies. Each mode determines how events are
   * handled, balancing between immediacy and resource efficiency.
   *
   * <ul>
   *   <li>{@code SYNC} - Events are processed synchronously, immediately after the associated
   *       operation completes. While this approach ensures prompt handling of events, it may block
   *       the main process if the event listener requires significant time to process an event.
   *   <li>{@code ASYNC_ISOLATED} - Events are handled asynchronously with each listener possessing
   *       its own distinct event-processing queue. This mode allows for customized and isolated
   *       event processing but may increase resource consumption due to the necessity of a
   *       dedicated event dispatcher for each listener.
   *   <li>{@code ASYNC_SHARED} - In this mode, event listeners share a common event-processing
   *       queue, processing events asynchronously. This approach enhances resource efficiency by
   *       utilizing a shared dispatcher for handling events across multiple listeners.
   * </ul>
   */
  enum Mode {
    SYNC,
    ASYNC_ISOLATED,
    ASYNC_SHARED
  }

  /**
   * Initializes the plugin with the given set of properties. This phase may involve setting up
   * necessary resources for the plugin's functionality.
   *
   * <p>Failure during this phase will prevent the server from starting, highlighting a critical
   * setup issue with the plugin.
   *
   * @param properties A map of properties used for initializing the plugin.
   * @throws RuntimeException Indicates a critical failure in plugin setup, preventing server
   *     startup.
   */
  void init(Map<String, String> properties) throws RuntimeException;

  /**
   * Starts the plugin, transitioning it to a ready state. This method is invoked after successful
   * initialization.
   *
   * <p>A failure to start indicates an inability for the plugin to enter its operational state,
   * which will prevent the server from starting.
   *
   * @throws RuntimeException Indicates a failure to start the plugin, blocking the server's launch.
   */
  void start() throws RuntimeException;

  /**
   * Stops the plugin, releasing any resources that were allocated during its operation. This method
   * aims to ensure a clean termination, mitigating potential resource leaks or incomplete
   * shutdowns.
   *
   * <p>While the server's operation is unaffected by exceptions thrown during this process, failing
   * to properly stop may lead to resource management issues.
   *
   * @throws RuntimeException Indicates issues during the stopping process, potentially leading to
   *     resource leaks.
   */
  void stop() throws RuntimeException;

  /**
   * Handle post-events generated after the completion of an operation.
   *
   * <p>This method provides a hook for post-operation event processing, you couldn't change the
   * resource in the event.
   *
   * @param postEvent The post event to be processed.
   * @throws RuntimeException Indicates issues encountered during event processing, this has no
   *     affect to the operation.
   */
  default void onPostEvent(Event postEvent) throws RuntimeException {}

  /**
   * Handle pre-events generated before the operation.
   *
   * <p>This method handles pre-operation events in SYNC or ASYNC mode, any changes to resources in
   * the event will affect the subsequent operations.
   *
   * @param preEvent The pre event to be processed.
   * @throws ForbiddenException The subsequent operation will be skipped if and only if the event
   *     listener throwing {@code org.apache.gravitino.exceptions.ForbiddenException} and the event
   *     listener is SYNC mode, the exception will be ignored and logged only in other conditions.
   */
  default void onPreEvent(PreEvent preEvent) throws ForbiddenException {}

  /**
   * Transforms a pre-event before listener processing.
   *
   * <p>Plugins can modify the pre-event through this method. Transformation order follows the event
   * listener configuration sequence.
   *
   * @param preEvent Pre-event to transform
   * @return Transformed pre-event
   */
  default @NotNull SupportsChangingPreEvent transformPreEvent(SupportsChangingPreEvent preEvent) {
    return preEvent;
  }

  /**
   * Specifies the default operational mode for event processing by the plugin. The default
   * implementation is synchronous, but implementers can override this to utilize asynchronous
   * processing modes.
   *
   * @return The operational {@link Mode} of the plugin for event processing.
   */
  default Mode mode() {
    return Mode.SYNC;
  }
}
