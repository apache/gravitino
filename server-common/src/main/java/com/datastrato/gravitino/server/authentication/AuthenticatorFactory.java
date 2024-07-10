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

package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is responsible for creating instances of Authenticator implementations. */
public class AuthenticatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticatorFactory.class);

  private static final Splitter COMMA = Splitter.on(",").omitEmptyStrings().trimResults();

  public static final ImmutableMap<String, String> AUTHENTICATORS =
      ImmutableMap.of(
          AuthenticatorType.SIMPLE.name().toLowerCase(),
          SimpleAuthenticator.class.getCanonicalName(),
          AuthenticatorType.OAUTH.name().toLowerCase(),
          OAuth2TokenAuthenticator.class.getCanonicalName(),
          AuthenticatorType.KERBEROS.name().toLowerCase(),
          KerberosAuthenticator.class.getCanonicalName());

  private AuthenticatorFactory() {}

  public static List<Authenticator> createAuthenticators(Config config) {
    String configAuthenticator = config.get(Configs.AUTHENTICATOR);
    List<String> names = COMMA.splitToList(configAuthenticator);

    List<Authenticator> authenticators = Lists.newArrayList();
    for (String name : names) {
      String className = AUTHENTICATORS.getOrDefault(name, name);
      try {
        Authenticator authenticator =
            (Authenticator) Class.forName(className).getDeclaredConstructor().newInstance();
        authenticators.add(authenticator);
      } catch (Exception e) {
        LOG.error("Failed to create and initialize Authenticator by name {}.", name, e);
        throw new RuntimeException("Failed to create and initialize Authenticator: " + name, e);
      }
    }
    return authenticators;
  }
}
