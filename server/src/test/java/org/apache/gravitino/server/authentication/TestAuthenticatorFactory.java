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

import java.util.Collections;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.auth.local.BasicAuthenticator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuthenticatorFactory {

  @Test
  public void testCreateBasicAuthenticator() {
    Config config = new Config(false) {};
    config.set(
        Configs.AUTHENTICATORS,
        Collections.singletonList(AuthenticatorType.BASIC.name().toLowerCase()));

    List<Authenticator> authenticators = AuthenticatorFactory.createAuthenticators(config);

    Assertions.assertEquals(1, authenticators.size());
    Assertions.assertTrue(authenticators.get(0) instanceof BasicAuthenticator);
  }
}
