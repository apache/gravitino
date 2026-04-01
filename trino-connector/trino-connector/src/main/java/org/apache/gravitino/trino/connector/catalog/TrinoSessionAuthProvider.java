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
import java.util.Base64;
import javax.annotation.Nullable;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.client.CustomTokenProvider;

/**
 * A {@link CustomTokenProvider} that forwards the Trino session user's credentials to Gravitino on
 * every request by storing them in a {@link ThreadLocal}.
 *
 * <p>There are two operating modes, selected at construction time:
 *
 * <ul>
 *   <li><b>OAUTH2_TOKEN</b> – reads a Bearer token from the Trino session's extra credentials using
 *       a configured key, and forwards it as {@code Authorization: Bearer <token>}.
 *   <li><b>SIMPLE_SESSION</b> – encodes the Trino session username using the same Basic-auth format
 *       as {@code SimpleTokenProvider}, so the Gravitino server sees the actual Trino user rather
 *       than a shared service account.
 * </ul>
 *
 * <p>When no session has been applied (e.g. during connector start-up), {@link #hasTokenData()}
 * returns {@code false} so that the request is sent without an {@code Authorization} header.
 *
 * @since 1.3.0
 */
public class TrinoSessionAuthProvider extends CustomTokenProvider {

  /** Distinguishes between the two operating modes. */
  enum Mode {
    OAUTH2_TOKEN,
    SIMPLE_SESSION
  }

  private static final ThreadLocal<byte[]> TOKEN_HOLDER = new ThreadLocal<>();

  private final Mode mode;

  /** The key to look up in Trino extra credentials (only used in OAUTH2_TOKEN mode). */
  @Nullable private final String credentialKey;

  /**
   * Creates a provider in OAUTH2_TOKEN mode.
   *
   * @param credentialKey the key whose value in the Trino session's extra credentials is the Bearer
   *     token
   */
  TrinoSessionAuthProvider(String credentialKey) {
    this.mode = Mode.OAUTH2_TOKEN;
    this.credentialKey = credentialKey;
    this.schemeName = "Bearer"; // required by parent abstract; not used because we override
  }

  /**
   * Creates a provider in SIMPLE_SESSION mode. The Trino session user name is encoded using the
   * same Basic-auth format as {@code SimpleTokenProvider}.
   */
  TrinoSessionAuthProvider() {
    this.mode = Mode.SIMPLE_SESSION;
    this.credentialKey = null;
    this.schemeName = "Basic"; // required by parent abstract; not used because we override
  }

  /**
   * Reads credentials from the current session and stores them in the per-thread holder.
   *
   * @param session the current Trino connector session
   */
  void applySession(ConnectorSession session) {
    byte[] token;
    if (mode == Mode.OAUTH2_TOKEN) {
      String bearerToken = session.getIdentity().getExtraCredentials().get(credentialKey);
      token =
          bearerToken != null
              ? (AuthConstants.AUTHORIZATION_BEARER_HEADER + bearerToken)
                  .getBytes(StandardCharsets.UTF_8)
              : null;
    } else {
      token = encodeSimpleToken(session.getUser());
    }

    if (token != null) {
      TOKEN_HOLDER.set(token);
    } else {
      TOKEN_HOLDER.remove();
    }
  }

  /** Removes the per-thread credentials. Must be called at the end of each query. */
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

  private static byte[] encodeSimpleToken(String user) {
    String userInformation = user + ":dummy";
    return (AuthConstants.AUTHORIZATION_BASIC_HEADER
            + new String(
                Base64.getEncoder().encode(userInformation.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8))
        .getBytes(StandardCharsets.UTF_8);
  }
}
