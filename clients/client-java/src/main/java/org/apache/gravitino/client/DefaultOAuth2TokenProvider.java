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
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;

/** This class is the default implement of OAuth2TokenProvider. */
public class DefaultOAuth2TokenProvider extends OAuth2TokenProvider {

  private String credential;
  private String scope;
  private String path;
  private String token;

  private DefaultOAuth2TokenProvider() {}

  @Override
  protected String getAccessToken() {
    synchronized (this) {
      Long expires = OAuth2ClientUtil.expiresAtMillis(token);
      if (expires == null || expires <= System.currentTimeMillis()) {
        token =
            OAuth2ClientUtil.fetchToken(client, Collections.emptyMap(), credential, scope, path)
                .getAccessToken();
      }
      return token;
    }
  }

  /**
   * Creates a new instance of the DefaultOAuth2TokenProvider.Builder
   *
   * @return A new instance of DefaultOAuth2TokenProvider.Builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for configuring and creating instances of DefaultOAuth2TokenProvider. */
  public static class Builder
      extends OAuth2TokenProviderBuilder<Builder, DefaultOAuth2TokenProvider> {

    private String credential;
    private String scope;
    private String path;

    /**
     * Sets the scope for the HTTP token requests.
     *
     * @param scope The scope for the HTTP token requests.
     * @return This Builder instance for method chaining.
     */
    public Builder withScope(String scope) {
      this.scope = scope;
      return this;
    }

    /**
     * Sets the path for the HTTP token requests.
     *
     * @param path The path for the HTTP token requests.
     * @return This Builder instance for method chaining.
     */
    public Builder withPath(String path) {
      this.path = path;
      return this;
    }

    /**
     * Sets the credential for the HTTP token requests.
     *
     * @param credential The credential for the HTTP token requests.
     * @return This Builder instance for method chaining.
     */
    public Builder withCredential(String credential) {
      this.credential = credential;
      return this;
    }

    @Override
    protected DefaultOAuth2TokenProvider internalBuild() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(credential), "OAuth2TokenProvider must set credential");

      Preconditions.checkArgument(
          StringUtils.isNotBlank(scope), "OAuth2TokenProvider must set scope");

      Preconditions.checkArgument(
          StringUtils.isNotBlank(path), "OAuth2TokenProvider must set path");

      DefaultOAuth2TokenProvider provider = new DefaultOAuth2TokenProvider();
      provider.client = client;
      provider.credential = credential;
      provider.scope = scope;
      provider.path = path;
      provider.token =
          OAuth2ClientUtil.fetchToken(client, Collections.emptyMap(), credential, scope, path)
              .getAccessToken();
      return provider;
    }
  }
}
