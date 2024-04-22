/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.auth.AuthenticatorType;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Locale;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestServerAuthenticator extends KerberosSecurityTestcase {

  @BeforeEach
  public void setup() throws Exception {
    startMiniKdc();
  }

  @AfterEach
  public void teardown() throws Exception {
    stopMiniKdc();
  }

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
    config.set(Configs.AUTHENTICATOR, AuthenticatorType.SIMPLE.name().toLowerCase(Locale.ROOT));
    ServerAuthenticator serverAuthenticator = ServerAuthenticator.getInstance();
    serverAuthenticator.initialize(config);
    Assertions.assertEquals(1, serverAuthenticator.authenticators().length);
    Assertions.assertEquals(
        AuthenticatorType.SIMPLE.name().toLowerCase(Locale.ROOT),
        serverAuthenticator.authenticators()[0].name());
  }

  @Test
  public void testMultiFilter() throws Exception {
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
    config.set(Configs.AUTHENTICATOR, authenticators);
    initOAuthConfig(config);
    initKerberosConfig(config);

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
    config.set(Configs.AUTHENTICATOR, "unknown");
    ServerAuthenticator serverAuthenticator = ServerAuthenticator.getInstance();
    Assertions.assertThrows(RuntimeException.class, () -> serverAuthenticator.initialize(config));
  }

  private void initOAuthConfig(Config config) {
    config.set(OAuthConfig.SERVICE_AUDIENCE, "mock service audience");
    config.set(OAuthConfig.ALLOW_SKEW_SECONDS, 100L);
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
    String publicKey =
        new String(
            Base64.getEncoder().encode(keyPair.getPublic().getEncoded()), StandardCharsets.UTF_8);
    config.set(OAuthConfig.DEFAULT_SIGN_KEY, publicKey);
    config.set(OAuthConfig.DEFAULT_TOKEN_PATH, "test");
    config.set(OAuthConfig.DEFAULT_SERVER_URI, "test");
  }

  private void initKerberosConfig(Config config) throws Exception {
    File keytabFile = new File(KerberosTestUtils.getKeytabFile());
    config.set(KerberosConfig.PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    config.set(KerberosConfig.KEYTAB, KerberosTestUtils.getKeytabFile());
    String clientPrincipal = removeRealm(KerberosTestUtils.getClientPrincipal());
    String serverPrincipal = removeRealm(KerberosTestUtils.getServerPrincipal());
    getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
  }

  private String removeRealm(String principal) {
    return principal.substring(0, principal.lastIndexOf("@"));
  }
}
