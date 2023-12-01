/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.google.common.base.Preconditions;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;

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
      if (expires == null) {
        return null;
      }
      if (expires > System.currentTimeMillis()) {
        return token;
      }
      token =
          OAuth2ClientUtil.fetchToken(client, Collections.emptyMap(), credential, scope, path)
              .getAccessToken();
      return token;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

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
          StringUtils.isNotBlank(credential), "OAuthDataProvider must contain credential");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(scope), "OAuthDataProvider must contain scope");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(path), "OAuthDataProvider must contain path");
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
