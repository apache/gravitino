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
package org.apache.gravitino.integration.test.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.gravitino.client.OAuth2TokenProvider;

public class OAuthMockDataProvider extends OAuth2TokenProvider {

  private static class InstanceHolder {
    private static final OAuthMockDataProvider INSTANCE = new OAuthMockDataProvider();
  }

  @Override
  protected String getAccessToken() {
    return new String(token, StandardCharsets.UTF_8);
  }

  public static OAuthMockDataProvider getInstance() {
    return InstanceHolder.INSTANCE;
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
