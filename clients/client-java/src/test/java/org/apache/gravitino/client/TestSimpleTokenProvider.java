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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleTokenProvider {

  @Test
  public void testAuthentication() throws IOException {
    try (AuthDataProvider provider = new SimpleTokenProvider()) {
      Assertions.assertTrue(provider.hasTokenData());
      String user = System.getenv("GRAVITINO_USER");
      String token = new String(provider.getTokenData(), StandardCharsets.UTF_8);
      Assertions.assertTrue(token.startsWith(AuthConstants.AUTHORIZATION_BASIC_HEADER));
      String tokenString =
          new String(
              Base64.getDecoder()
                  .decode(
                      token
                          .substring(AuthConstants.AUTHORIZATION_BASIC_HEADER.length())
                          .getBytes(StandardCharsets.UTF_8)),
              StandardCharsets.UTF_8);
      if (user != null) {
        Assertions.assertEquals(user + ":dummy", tokenString);
      } else {
        user = System.getProperty("user.name");
        Assertions.assertEquals(user + ":dummy", tokenString);
      }
    }
  }
}
