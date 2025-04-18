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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCustomTokenProvider {

  @Test
  public void testAuthentication() throws IOException {
    try (AuthDataProvider provider =
        CustomTestTokenProvider.builder().withSchemeName("test").withTokenInfo("test").build()) {
      Assertions.assertTrue(provider.hasTokenData());
      Assertions.assertArrayEquals(
          provider.getTokenData(), "test test".getBytes(StandardCharsets.UTF_8));
      // Token won't change
      Assertions.assertArrayEquals(
          provider.getTokenData(), "test test".getBytes(StandardCharsets.UTF_8));
    }
  }

  private static class CustomTestTokenProvider extends CustomTokenProvider {
    private String tokenInfo;

    private CustomTestTokenProvider() {}

    @Override
    protected String getCustomTokenInfo() {
      return tokenInfo;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder
        extends CustomTokenProviderBuilder<Builder, CustomTestTokenProvider> {
      private String tokenInfo;

      public Builder withTokenInfo(String tokenInfo) {
        this.tokenInfo = tokenInfo;
        return this;
      }

      @Override
      public Builder withSchemeName(String schemeName) {
        this.schemeName = schemeName;
        return this;
      }

      @Override
      protected CustomTestTokenProvider internalBuild() {
        CustomTestTokenProvider tokenProvider = new CustomTestTokenProvider();
        tokenProvider.schemeName = schemeName;
        tokenProvider.tokenInfo = tokenInfo;
        return tokenProvider;
      }
    }
  }
}
