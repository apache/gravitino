/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import com.datastrato.gravitino.client.auth.AuthDataProvider;
import java.io.IOException;

public class OAuthMockDataProvider implements AuthDataProvider {

  private static class InstanceHolder {
    private static final OAuthMockDataProvider INSTANCE = new OAuthMockDataProvider();
  }

  public static OAuthMockDataProvider getInstance() {
    return OAuthMockDataProvider.InstanceHolder.INSTANCE;
  }

  private byte[] token;

  @Override
  public boolean hasTokenData() {
    return true;
  }

  @Override
  public byte[] getTokenData() {
    return token;
  }

  @Override
  public void close() throws IOException {
    // no op
  }

  public void setTokenData(byte[] tokenData) {
    this.token = tokenData;
  }
}
