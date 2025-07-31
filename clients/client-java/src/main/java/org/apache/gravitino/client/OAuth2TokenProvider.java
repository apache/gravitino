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

package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.AuthConstants;

/**
 * OAuth2TokenProvider will request the access token from the authorization server and then provide
 * the access token for every request.
 */
public abstract class OAuth2TokenProvider implements AuthDataProvider {

  /** The HTTP client used to request the access token from the authorization server. */
  protected HTTPClient client;

  /**
   * Judge whether AuthDataProvider can provide token data.
   *
   * @return true if the AuthDataProvider can provide token data otherwise false.
   */
  @Override
  public boolean hasTokenData() {
    return true;
  }

  /**
   * Acquire the data of token for authentication. The client will set the token data as HTTP header
   * Authorization directly. So the return value should ensure token data contain the token header
   * (eg: Bearer, Basic) if necessary.
   *
   * @return the token data is used for authentication.
   */
  @Override
  public byte[] getTokenData() {
    String accessToken = getAccessToken();
    if (accessToken == null) {
      return null;
    }
    return (AuthConstants.AUTHORIZATION_BEARER_HEADER + accessToken)
        .getBytes(StandardCharsets.UTF_8);
  }

  /** Closes the OAuth2TokenProvider and releases any underlying resources. */
  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  /**
   * Get the access token from the authorization server.
   *
   * @return The access token.
   */
  protected abstract String getAccessToken();

  interface Builder<SELF extends Builder<SELF, T>, T extends OAuth2TokenProvider> {
    SELF withUri(String uri);

    T build();
  }

  /**
   * Builder interface for creating instances of {@link OAuth2TokenProvider}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the OAuth2TokenProvider being built.
   */
  public abstract static class OAuth2TokenProviderBuilder<
          SELF extends Builder<SELF, T>, T extends OAuth2TokenProvider>
      implements Builder<SELF, T> {

    private String uri;
    /** The HTTP client used to request the access token from the authorization server. */
    protected HTTPClient client;

    /**
     * Sets the uri of the OAuth2TokenProvider
     *
     * @param uri The uri of oauth server .
     * @return The builder instance.
     */
    @Override
    public SELF withUri(String uri) {
      this.uri = uri;
      return self();
    }

    /**
     * Builds the instance of the OAuth2TokenProvider.
     *
     * @return The built OAuth2TokenProvider instance.
     */
    @Override
    public T build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(uri), "OAuth2TokenProvider must set url");
      client = HTTPClient.builder(Collections.emptyMap()).uri(uri).build();
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    /**
     * Builds the instance of the OAuth2TokenProvider.
     *
     * @return The built OAuth2TokenProvider instance.
     */
    protected abstract T internalBuild();
  }
}
