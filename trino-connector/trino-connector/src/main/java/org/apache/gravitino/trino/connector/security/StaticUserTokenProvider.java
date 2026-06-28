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

import org.apache.gravitino.client.OAuth2TokenProvider;

/**
 * An {@link OAuth2TokenProvider} that returns a pre-fetched, already-valid access token rather than
 * minting one via client credentials. Used for per-user session forwarding: the end user's IdP
 * access token, forwarded by Trino into the connector session, is presented directly to Gravitino
 * so the server authorizes against the end user's identity instead of a shared service identity.
 *
 * <p>The raw token is returned from {@link #getAccessToken()}. The {@code Bearer } prefix is added
 * by {@link OAuth2TokenProvider#getTokenData()}, so the token held here must not include it.
 */
public final class StaticUserTokenProvider extends OAuth2TokenProvider {

  private final String accessToken;

  /**
   * Constructs a provider that always returns the given access token.
   *
   * @param accessToken the raw bearer token (without the {@code Bearer } prefix) to present to
   *     Gravitino
   */
  public StaticUserTokenProvider(String accessToken) {
    this.accessToken = accessToken;
  }

  @Override
  protected String getAccessToken() {
    return accessToken;
  }
}
