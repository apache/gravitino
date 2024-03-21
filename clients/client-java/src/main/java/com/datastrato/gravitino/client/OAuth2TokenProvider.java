/*
 * Copyright 2023 Datastrato Pvt Ltd.
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
    return (AuthConstants.AUTHORIZATION_BEARER_HEADER + getAccessToken())
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
