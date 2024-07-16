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
package org.apache.gravitino.server.authentication;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.gravitino.Config;
import org.apache.gravitino.auth.AuthConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSimpleAuthenticator {

  @Test
  public void testAuthentication() {
    SimpleAuthenticator simpleAuthenticator = new SimpleAuthenticator();
    Config config = new Config(false) {};
    simpleAuthenticator.initialize(config);
    Assertions.assertTrue(simpleAuthenticator.isDataFromToken());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER, simpleAuthenticator.authenticateToken(null).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator.authenticateToken("".getBytes(StandardCharsets.UTF_8)).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator.authenticateToken("abc".getBytes(StandardCharsets.UTF_8)).getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator
            .authenticateToken(
                AuthConstants.AUTHORIZATION_BASIC_HEADER.getBytes(StandardCharsets.UTF_8))
            .getName());
    Assertions.assertEquals(
        AuthConstants.ANONYMOUS_USER,
        simpleAuthenticator
            .authenticateToken(
                (AuthConstants.AUTHORIZATION_BASIC_HEADER + "xx").getBytes(StandardCharsets.UTF_8))
            .getName());
    Assertions.assertEquals(
        "gravitino",
        simpleAuthenticator
            .authenticateToken(
                (AuthConstants.AUTHORIZATION_BASIC_HEADER
                        + new String(
                            Base64.getEncoder()
                                .encode("gravitino:gravitino".getBytes(StandardCharsets.UTF_8)),
                            StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_8))
            .getName());
  }
}
