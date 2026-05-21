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

package org.apache.gravitino.spark.connector.auth;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.CustomTokenProvider;

/** Provides a Bearer token by reading a token file for each Gravitino client request. */
public final class BearerTokenFileProvider extends CustomTokenProvider {
  private static final String BEARER_PREFIX = "Bearer ";

  private final Path tokenFile;

  public BearerTokenFileProvider(String tokenFile) {
    if (StringUtils.isBlank(tokenFile)) {
      throw new IllegalArgumentException("tokenFile cannot be blank");
    }

    this.schemeName = "Bearer";
    this.tokenFile = new File(tokenFile).toPath();
  }

  @Override
  protected String getCustomTokenInfo() {
    try {
      String token = new String(Files.readAllBytes(tokenFile), StandardCharsets.UTF_8).trim();
      if (token.startsWith(BEARER_PREFIX)) {
        token = token.substring(BEARER_PREFIX.length()).trim();
      }

      if (StringUtils.isBlank(token)) {
        throw new IllegalStateException("Bearer token file is empty: " + tokenFile);
      }

      return token;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read Bearer token file: " + tokenFile, e);
    }
  }
}
