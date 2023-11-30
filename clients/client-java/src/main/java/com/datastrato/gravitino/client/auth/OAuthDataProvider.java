/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import com.datastrato.gravitino.client.HTTPClient;
import com.datastrato.gravitino.dto.responses.OAuthTokenResponse;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * OAuthDataProvider will request the access token from the authorization server and then provide
 * the access token for every request.
 */
public class OAuthDataProvider implements AuthDataProvider {

  private String token;

  private String credential;
  private String uri;
  private String scope;
  private HTTPClient client;
  private String path;
  private boolean isInitialized = false;

  @Override
  public void initialize(Map<String, String> properties) {
    this.credential = properties.get(OAuth2ClientUtil.CREDENTIAL);
    Preconditions.checkArgument(StringUtils.isNotBlank(this.credential), "OAuthDataProvider must contain credential");
    this.uri = properties.get(OAuth2ClientUtil.URI);
    Preconditions.checkArgument(StringUtils.isNotBlank(this.uri), "OAuthDataProvider must contain url");
    this.scope = properties.get(OAuth2ClientUtil.SCOPE);
    Preconditions.checkArgument(StringUtils.isNotBlank(this.scope), "OAuthDataProvider must contain scope");
    this.client = HTTPClient.builder(Collections.emptyMap()).uri(this.uri).build();
    this.path = properties.get(OAuth2ClientUtil.PATH);
    Preconditions.checkArgument(StringUtils.isNotBlank(this.path), "OAuthDataProvider must contain path");
    OAuthTokenResponse response =
        OAuth2ClientUtil.fetchToken(client, Collections.emptyMap(), credential, scope, path);
    response.validate();
    this.token = response.getAccessToken();
    this.isInitialized = true;
  }

  @Override
  public boolean hasTokenData() {
    if (!isInitialized) {
      throw new IllegalStateException("OAuthDataProvider isn't initialized yet");
    }
    return true;
  }

  @Override
  public byte[] getTokenData() {
    if (!isInitialized) {
      throw new IllegalStateException("OAuthDataProvider isn't initialized yet");
    }
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
}
