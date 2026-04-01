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

import io.trino.spi.connector.ConnectorSession;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.client.CustomTokenProvider;

/**
 * A {@link CustomTokenProvider} that forwards the Trino session's per-query OAuth2 Bearer token to
 * Gravitino on every request by storing it in a {@link ThreadLocal}.
 *
 * <p>The token is read from the Trino session's extra credentials using a configured key and
 * forwarded as {@code Authorization: Bearer <token>}.
 *
 * <p>When no session has been applied (e.g. during connector start-up), {@link #hasTokenData()}
 * returns {@code false} so that the request is sent without an {@code Authorization} header.
 *
 * @since 1.3.0
 */
public class TrinoSessionAuthProvider extends CustomTokenProvider {

  private static final ThreadLocal<byte[]> TOKEN_HOLDER = new ThreadLocal<>();

  /** The key to look up in Trino extra credentials that holds the Bearer token. */
  private final String credentialKey;

  /**
   * Creates a provider that reads a Bearer token from the Trino session's extra credentials using
   * the given key.
   *
   * @param credentialKey the key whose value in the Trino session's extra credentials is the Bearer
   *     token
   */
  TrinoSessionAuthProvider(String credentialKey) {
    this.credentialKey = credentialKey;
    this.schemeName = "Bearer"; // required by parent abstract; not used because we override
  }

  /**
   * Reads the Bearer token from the current session's extra credentials and stores it in the
   * per-thread holder.
   *
   * @param session the current Trino connector session
   */
  void applySession(ConnectorSession session) {
    String bearerToken = session.getIdentity().getExtraCredentials().get(credentialKey);
    if (bearerToken != null) {
      TOKEN_HOLDER.set(
          (AuthConstants.AUTHORIZATION_BEARER_HEADER + bearerToken)
              .getBytes(StandardCharsets.UTF_8));
    } else {
      TOKEN_HOLDER.remove();
    }
  }

  /** Removes the per-thread token. Must be called at the end of each query. */
  void clearSession() {
    TOKEN_HOLDER.remove();
  }

  @Override
  public boolean hasTokenData() {
    return TOKEN_HOLDER.get() != null;
  }

  @Override
  public byte[] getTokenData() {
    return TOKEN_HOLDER.get();
  }

  /**
   * Required by the abstract parent. Not called because {@link #getTokenData()} is overridden
   * directly; returns the raw token string for completeness.
   */
  @Override
  protected String getCustomTokenInfo() {
    byte[] token = TOKEN_HOLDER.get();
    return token != null ? new String(token, StandardCharsets.UTF_8) : null;
  }

  @Override
  public void close() throws IOException {
    TOKEN_HOLDER.remove();
  }
}
