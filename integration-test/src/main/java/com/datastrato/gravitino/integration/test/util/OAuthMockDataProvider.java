/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import com.datastrato.gravitino.client.OAuth2TokenProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OAuthMockDataProvider extends OAuth2TokenProvider {

  private static class InstanceHolder {
    private static final OAuthMockDataProvider INSTANCE = new OAuthMockDataProvider();
  }

  @Override
  protected String getAccessToken() {
    return new String(token, StandardCharsets.UTF_8);
  }

  public static OAuthMockDataProvider getInstance() {
    return OAuthMockDataProvider.InstanceHolder.INSTANCE;
  }

  private byte[] token;

  /** Close the resource of OAuthTokenProvider */
  @Override
  public void close() throws IOException {
    // no op
  }

  public void setTokenData(byte[] tokenData) {
    this.token = tokenData;
  }
}
