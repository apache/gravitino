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
import java.util.Base64;
import org.apache.gravitino.auth.AuthConstants;

/**
 * SimpleAuthProvider will use the environment variable {@code GRAVITINO_USER} or the user of the
 * system to generate a basic token for every request.
 */
final class SimpleTokenProvider implements AuthDataProvider {

  private final byte[] token;

  /** Creates a SimpleTokenProvider using the {@code GRAVITINO_USER} env var or system username. */
  public SimpleTokenProvider() {
    String gravitinoUser = System.getenv("GRAVITINO_USER");
    if (gravitinoUser == null) {
      gravitinoUser = System.getProperty("user.name");
    }
    this.token = buildToken(gravitinoUser, "dummy");
  }

  /**
   * Creates a SimpleTokenProvider with an explicit username.
   *
   * @param gravitinoUser The username to authenticate with.
   */
  public SimpleTokenProvider(String gravitinoUser) {
    this.token = buildToken(gravitinoUser, "dummy");
  }

  /**
   * Creates a SimpleTokenProvider with an explicit username and password.
   *
   * @param gravitinoUser The username to authenticate with.
   * @param password The password to authenticate with.
   */
  public SimpleTokenProvider(String gravitinoUser, String password) {
    this.token = buildToken(gravitinoUser, password);
  }

  private static byte[] buildToken(String gravitinoUser, String password) {
    String userInformation = gravitinoUser + ":" + password;
    return (AuthConstants.AUTHORIZATION_BASIC_HEADER
            + new String(
                Base64.getEncoder().encode(userInformation.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8))
        .getBytes(StandardCharsets.UTF_8);
  }

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
}
