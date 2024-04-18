/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.auth.AuthenticatorType;
import java.util.Locale;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestServerAuthenticator {

  @Test
  public void testDefaultFilter() {
    Config config = new Config(false) {};
    ServerAuthenticator serverAuthenticator = ServerAuthenticator.getInstance();
    serverAuthenticator.initialize(config);
    Assertions.assertEquals(1, serverAuthenticator.authenticators().length);
    Assertions.assertEquals(
        AuthenticatorType.SIMPLE.name().toLowerCase(Locale.ROOT),
        serverAuthenticator.authenticators()[0].name());
  }

  @Test
  public void testSingleFilter() {
    Config config = new Config(false) {};
    config.set(OAuthConfig.AUTHENTICATOR, AuthenticatorType.SIMPLE.name().toLowerCase(Locale.ROOT));
    ServerAuthenticator serverAuthenticator = ServerAuthenticator.getInstance();
    serverAuthenticator.initialize(config);
    Assertions.assertEquals(1, serverAuthenticator.authenticators().length);
    Assertions.assertEquals(
        AuthenticatorType.SIMPLE.name().toLowerCase(Locale.ROOT),
        serverAuthenticator.authenticators()[0].name());
  }

  @Test
  public void testMultiFilter() {
    Config config = new Config(false) {};
    String authenticators =
        new StringBuilder()
            .append(AuthenticatorType.SIMPLE.name().toLowerCase(Locale.ROOT))
            .append(",   ")
            .append(AuthenticatorType.OAUTH.name().toLowerCase(Locale.ROOT))
            .append(",  ")
            .append(AuthenticatorType.KERBEROS.name().toLowerCase())
            .append(",")
            .toString();
    config.set(OAuthConfig.AUTHENTICATOR, authenticators);
    config.set(OAuthConfig.SERVICE_AUDIENCE, "mock service audience");
    config.set(OAuthConfig.ALLOW_SKEW_SECONDS, 100L);
    config.set(KerberosConfig.PRINCIPAL, "HTTP//XXX");
    config.set(KerberosConfig.KEYTAB, "s_gravitino.keytab");
    ServerAuthenticator serverAuthenticator = ServerAuthenticator.getInstance();
    serverAuthenticator.initialize(config);
    Assertions.assertEquals(3, serverAuthenticator.authenticators().length);
    Assertions.assertEquals(
        AuthenticatorType.SIMPLE.name().toLowerCase(Locale.ROOT),
        serverAuthenticator.authenticators()[0].name());
    Assertions.assertEquals(
        AuthenticatorType.OAUTH.name().toLowerCase(Locale.ROOT),
        serverAuthenticator.authenticators()[1].name());
    Assertions.assertEquals(
        AuthenticatorType.KERBEROS.name().toLowerCase(Locale.ROOT),
        serverAuthenticator.authenticators()[2].name());
  }

  @Test
  public void testUnknownFilter() {
    Config config = new Config(false) {};
    config.set(OAuthConfig.AUTHENTICATOR, "inknown");
    ServerAuthenticator serverAuthenticator = ServerAuthenticator.getInstance();
    Assertions.assertThrows(RuntimeException.class, () -> serverAuthenticator.initialize(config));
  }
}
