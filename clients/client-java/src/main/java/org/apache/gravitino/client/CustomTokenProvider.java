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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;

/** CustomTokenProvider will provide a custom access token for every request. */
public abstract class CustomTokenProvider implements AuthDataProvider {
  private static final String BLANK_SPACE = " ";

  /** The name of authentication scheme. */
  protected String schemeName;

  @Override
  public boolean hasTokenData() {
    return true;
  }

  @Override
  public byte[] getTokenData() {
    return (schemeName + BLANK_SPACE + getCustomTokenInfo()).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }

  /**
   * Gets the custom token information.
   *
   * @return The custom token information.
   */
  protected abstract String getCustomTokenInfo();

  /**
   * Builder interface for configuring and creating instances of CustomTokenProvider.
   *
   * @param <SELF> The subclass type of the Builder.
   * @param <T> The subclass type of the CustomTokenProvider.
   */
  protected interface Builder<SELF extends Builder<SELF, T>, T extends CustomTokenProvider> {
    /**
     * Sets the scheme name for the CustomTokenProvider.
     *
     * @param schemeName The scheme name for the CustomTokenProvider.
     * @return This Builder instance for method chaining.
     */
    SELF withSchemeName(String schemeName);

    /**
     * Builds the instance of the CustomTokenProvider.
     *
     * @return The instance of the CustomTokenProvider.
     */
    T build();
  }

  /** Builder class for configuring and creating instances of CustomTokenProvider. */
  public abstract static class CustomTokenProviderBuilder<
          SELF extends Builder<SELF, T>, T extends CustomTokenProvider>
      implements Builder<SELF, T> {

    /** the name of authentication scheme */
    protected String schemeName;

    /**
     * Sets the scheme name for the CustomTokenProvider.
     *
     * <p>Scheme name is the name of the authentication scheme. For example, "Bearer" or "Basic".
     *
     * @param schemeName The scheme name for the CustomTokenProvider.
     * @return This Builder instance for method chaining.
     */
    public SELF withSchemeName(String schemeName) {
      this.schemeName = schemeName;
      return self();
    }

    /**
     * Builds the instance of the CustomTokenProvider.
     *
     * @return The instance of the CustomTokenProvider.
     */
    public T build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(schemeName), "CustomTokenProvider must set schemeName");

      T t = internalBuild();
      return t;
    }

    /**
     * Builds the instance of the CustomTokenProvider.
     *
     * @return The built CustomTokenProvider instance.
     */
    protected abstract T internalBuild();

    private SELF self() {
      return (SELF) this;
    }
  }
}
