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

import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;

/** Defines the contract for dispatching events to registered listeners. */
public interface EventDispatcher {

  /**
   * Dispatches a {@link PreEvent} to the appropriate listener.
   *
   * @param event the pre-event to be dispatched.
   * @throws ForbiddenException if the dispatch is not permitted by the plugin.
   */
  void dispatchPreEvent(PreEvent event) throws ForbiddenException;

  /**
   * Dispatches a {@link Event} (post-event) to the appropriate listener.
   *
   * @param event the post-event to be dispatched.
   */
  void dispatchPostEvent(Event event);
}
