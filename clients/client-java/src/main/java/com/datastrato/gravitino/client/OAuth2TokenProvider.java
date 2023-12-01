/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.auth.AuthConstants;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;

/**
 * OAuthDataProvider will request the access token from the authorization server and then provide
 * the access token for every request.
 */
public abstract class OAuth2TokenProvider implements AuthDataProvider {

  protected HTTPClient client;

  @Override
  public boolean hasTokenData() {
    return true;
  }

  @Override
  public byte[] getTokenData() {
    String accessToken = getAccessToken();
    if (accessToken == null) {
      return null;
    }
    return (AuthConstants.AUTHORIZATION_BEARER_HEADER + getAccessToken())
        .getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  protected abstract String getAccessToken();

  interface Builder<SELF extends Builder<SELF, T>, T extends OAuth2TokenProvider> {
    SELF withUri(String uri);

    T build();
  }

  public abstract static class OAuth2TokenProviderBuilder<
          SELF extends Builder<SELF, T>, T extends OAuth2TokenProvider>
      implements Builder<SELF, T> {

    private String uri;
    protected HTTPClient client;

    @Override
    public SELF withUri(String uri) {
      this.uri = uri;
      return self();
    }

    @Override
    public T build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uri), "OAuthDataProvider must contain url");
      client = HTTPClient.builder(Collections.emptyMap()).uri(uri).build();
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    protected abstract T internalBuild();
  }
}
