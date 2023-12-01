/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.client.HTTPClient;
import com.datastrato.gravitino.dto.responses.OAuthTokenResponse;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;

/**
 * OAuthDataProvider will request the access token from the authorization server and then provide
 * the access token for every request.
 */
public class OAuthDataProvider implements AuthDataProvider {

  private String token;

  private final String credential;
  private final String scope;
  private final HTTPClient client;
  private final String path;

  private OAuthDataProvider(String credential, String uri, String scope, String path) {
    this.credential = credential;
    this.scope = scope;
    this.client = HTTPClient.builder(Collections.emptyMap()).uri(uri).build();
    this.path = path;
    OAuthTokenResponse response =
        OAuth2ClientUtil.fetchToken(client, Collections.emptyMap(), credential, scope, path);
    response.validate();
    this.token = response.getAccessToken();
  }

  @Override
  public boolean hasTokenData() {
    return true;
  }

  @Override
  public byte[] getTokenData() {
    synchronized (this) {
      Long expires = OAuth2ClientUtil.expiresAtMillis(token);
      if (expires == null) {
        return null;
      }
      if (expires > System.currentTimeMillis()) {
        return token.getBytes(StandardCharsets.UTF_8);
      }
      token =
          OAuth2ClientUtil.fetchToken(client, Collections.emptyMap(), credential, scope, path)
              .getAccessToken();
      return token.getBytes(StandardCharsets.UTF_8);
    }
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public AuthenticatorType getAuthType() {
    return AuthenticatorType.OAUTH;
  }

  public static Builder builder(String uri) {
    return new Builder(uri);
  }

  public static class Builder {

    private final String uri;

    private String credential;
    private String scope;
    private String path;

    /**
     * Sets the base URI for the HTTP client.
     *
     * @param uri The base URI to be used for all HTTP requests.
     * @return This Builder instance for method chaining.
     */
    protected Builder(String uri) {
      this.uri = uri;
    }

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
     * @param credential The credential for the token HTTP request.
     * @return This Builder instance for method chaining.
     */
    public Builder withCredential(String credential) {
      this.credential = credential;
      return this;
    }

    public OAuthDataProvider build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uri), "OAuthDataProvider must contain url");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(credential), "OAuthDataProvider must contain credential");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(scope), "OAuthDataProvider must contain scope");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(path), "OAuthDataProvider must contain path");
      return new OAuthDataProvider(credential, uri, scope, path);
    }
  }
}
