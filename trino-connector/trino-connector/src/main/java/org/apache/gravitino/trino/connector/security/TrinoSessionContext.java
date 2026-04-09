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
package org.apache.gravitino.trino.connector.security;

import io.trino.spi.connector.ConnectorSession;

/**
 * Facade that aggregates all session-forwarding providers for a single Gravitino connector
 * instance.
 *
 * <p>{@link #applySession(ConnectorSession)} must be called at the start of each query (from {@code
 * ConnectorMetadata.beginQuery}) and {@link #clearSession()} at the end (from {@code
 * ConnectorMetadata.cleanupQuery}).
 *
 * @since 1.3.0
 */
public class TrinoSessionContext {

  private final TrinoSessionAuthProvider authProvider;

  TrinoSessionContext(TrinoSessionAuthProvider authProvider) {
    this.authProvider = authProvider;
  }

  /**
   * Populates per-thread credentials from the current session. Called once per query before any
   * Gravitino API request is made.
   *
   * @param session the current Trino connector session
   */
  public void applySession(ConnectorSession session) {
    authProvider.applySession(session);
  }

  /**
   * Clears the per-thread credentials. Must be called at the end of every query to prevent context
   * leaking to subsequent queries running on the same thread.
   */
  public void clearSession() {
    authProvider.clearSession();
  }
}
