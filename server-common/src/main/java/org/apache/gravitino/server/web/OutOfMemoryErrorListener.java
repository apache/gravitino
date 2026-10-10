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
package org.apache.gravitino.server.web;

import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/** Records out-of-memory failures before Jersey maps them to an HTTP response. */
public final class OutOfMemoryErrorListener
    implements ApplicationEventListener, RequestEventListener {
  private final ServerHealth health;

  /** Creates a listener using the shared server health state. */
  public OutOfMemoryErrorListener() {
    this(ServerHealth.getInstance());
  }

  /**
   * Creates a listener using the supplied health state.
   *
   * @param health the state to update on an out-of-memory failure
   */
  public OutOfMemoryErrorListener(ServerHealth health) {
    this.health = health;
  }

  /** {@inheritDoc} */
  @Override
  public void onEvent(ApplicationEvent event) {}

  /** {@inheritDoc} */
  @Override
  public RequestEventListener onRequest(RequestEvent event) {
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void onEvent(RequestEvent event) {
    if (event.getType() == RequestEvent.Type.ON_EXCEPTION) {
      health.recordFailure(event.getException());
    }
  }
}
