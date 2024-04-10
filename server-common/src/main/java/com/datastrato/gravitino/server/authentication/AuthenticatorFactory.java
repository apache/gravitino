/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is responsible for creating instances of Authenticator implementations. */
public class AuthenticatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticatorFactory.class);

  public static final ImmutableMap<String, String> AUTHENTICATORS =
      ImmutableMap.of(
          AuthenticatorType.SIMPLE.name().toLowerCase(),
          SimpleAuthenticator.class.getCanonicalName(),
          AuthenticatorType.OAUTH.name().toLowerCase(),
          OAuth2TokenAuthenticator.class.getCanonicalName(),
          AuthenticatorType.KERBEROS.name().toLowerCase(),
          KerberosAuthenticator.class.getCanonicalName());

  private AuthenticatorFactory() {}

  public static Authenticator createAuthenticator(Config config) {
    String name = config.get(Configs.AUTHENTICATOR);
    String className = AUTHENTICATORS.getOrDefault(name, name);

    try {
      return (Authenticator) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create and initialize Authenticator by name {}.", name, e);
      throw new RuntimeException("Failed to create and initialize Authenticator: " + name, e);
    }
  }
}
