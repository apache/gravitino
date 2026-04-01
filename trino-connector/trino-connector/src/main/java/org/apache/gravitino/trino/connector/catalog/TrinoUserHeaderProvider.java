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
package org.apache.gravitino.trino.connector.catalog;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorSession;
import java.util.Map;
import org.apache.gravitino.client.ExtraHeadersProvider;

/**
 * An {@link ExtraHeadersProvider} that forwards the Trino session user name to Gravitino via the
 * {@value #GRAVITINO_USER_HEADER} HTTP header on every request.
 *
 * <p>The user name is stored in a {@link ThreadLocal} and must be set by calling {@link
 * #applySession(ConnectorSession)} at the start of each query (typically from {@code
 * ConnectorMetadata.beginQuery}) and cleared by calling {@link #clearSession()} at the end (from
 * {@code ConnectorMetadata.cleanupQuery}).
 *
 * <p>When no session is active the provider returns an empty map, so the header is simply omitted
 * from start-up / background requests.
 *
 * @since 1.3.0
 */
public class TrinoUserHeaderProvider implements ExtraHeadersProvider {

  /** The HTTP header name used to forward the Trino session user to Gravitino. */
  public static final String GRAVITINO_USER_HEADER = "X-Gravitino-User";

  private static final ThreadLocal<String> USER_HOLDER = new ThreadLocal<>();

  /**
   * Stores the session user name for the current thread. Must be paired with a {@link
   * #clearSession()} call to avoid leaking user context across queries.
   *
   * @param session the current Trino connector session
   */
  void applySession(ConnectorSession session) {
    USER_HOLDER.set(session.getUser());
  }

  /** Removes the per-thread user name. Must be called at the end of each query. */
  void clearSession() {
    USER_HOLDER.remove();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns a singleton map containing {@value #GRAVITINO_USER_HEADER} when a session is active,
   * or an empty map otherwise.
   */
  @Override
  public Map<String, String> getHeaders() {
    String user = USER_HOLDER.get();
    return user != null ? ImmutableMap.of(GRAVITINO_USER_HEADER, user) : ImmutableMap.of();
  }
}
