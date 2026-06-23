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
package org.apache.gravitino.s3.credential.webidentity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * A {@link WebIdentityTokenSource} that reads the token from a file on disk. Matches the AWS IRSA /
 * Kubernetes projected service account token pattern.
 *
 * <p>The path is taken from {@code s3-web-identity-token-file}; if absent, the {@code
 * AWS_WEB_IDENTITY_TOKEN_FILE} environment variable is used as a fallback so existing IRSA setups
 * continue to work without configuration changes.
 *
 * <p>The token file is read on every call to {@link #getToken()} so that token rotations performed
 * by the kubelet (or any external rotator) are picked up automatically.
 */
public class FileWebIdentityTokenSource implements WebIdentityTokenSource {

  public static final String NAME = "file";
  static final String AWS_WEB_IDENTITY_TOKEN_FILE_ENV = "AWS_WEB_IDENTITY_TOKEN_FILE";

  private Path tokenFile;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    String configured = properties.get(WebIdentityTokenSourceConfig.FILE_PATH);
    String path =
        StringUtils.isNotBlank(configured)
            ? configured
            : System.getenv(AWS_WEB_IDENTITY_TOKEN_FILE_ENV);
    if (StringUtils.isBlank(path)) {
      throw new IllegalStateException(
          "No WebIdentity token file is configured. Set "
              + WebIdentityTokenSourceConfig.FILE_PATH
              + " or the "
              + AWS_WEB_IDENTITY_TOKEN_FILE_ENV
              + " environment variable.");
    }
    this.tokenFile = Paths.get(path);
  }

  @Override
  public String getToken() {
    if (!Files.exists(tokenFile)) {
      throw new IllegalStateException("WebIdentity token file does not exist: " + tokenFile);
    }
    try {
      String token = new String(Files.readAllBytes(tokenFile), StandardCharsets.UTF_8).trim();
      if (StringUtils.isBlank(token)) {
        throw new IllegalStateException("WebIdentity token file is empty: " + tokenFile);
      }
      return token;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read WebIdentity token file: " + tokenFile, e);
    }
  }

  @Override
  public void close() {}
}
