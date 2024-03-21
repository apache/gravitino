/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.auth;

import static com.datastrato.gravitino.server.auth.KerberosConfig.KEYTAB;
import static com.datastrato.gravitino.server.auth.KerberosConfig.PRINCIPAL;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.auth.KerberosUtils;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Callable;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestKerberosAuthenticator extends KerberosSecurityTestcase {

  @BeforeEach
  public void setup() throws Exception {
    startMiniKdc();
  }

  @AfterEach
  public void teardown() throws Exception {
    stopMiniKdc();
  }

  @Test
  public void testAuthenticatorInitialization() throws Exception {
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();

    // case 1: lack different parameters
    Config config = new Config(false) {};
    Exception e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              kerberosAuthenticator.initialize(config);
            });
    Assertions.assertTrue(e.getMessage().contains("The value can't be blank"));

    config.set(PRINCIPAL, "xx@xxx@");
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              kerberosAuthenticator.initialize(config);
            });
    Assertions.assertTrue(e.getMessage().contains("Principal must starts with"));

    config.set(PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              kerberosAuthenticator.initialize(config);
            });
    Assertions.assertTrue(e.getMessage().contains("The value can't be blank"));

    // case 2: keytab file doesn't exist
    config.set(KEYTAB, KerberosTestUtils.getKeytabFile());
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              kerberosAuthenticator.initialize(config);
            });
    Assertions.assertTrue(e.getMessage().contains("doesn't exist"));

    initKeyTab();
    kerberosAuthenticator.initialize(config);
  }

  @Test
  public void testAuthenticationWithException() throws Exception {
    // case 1: Empty token authorization header
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();
    Config config = new Config(false) {};
    config.set(PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    config.set(KEYTAB, KerberosTestUtils.getKeytabFile());
    initKeyTab();
    kerberosAuthenticator.initialize(config);
    Assertions.assertTrue(kerberosAuthenticator.isDataFromToken());
    Exception e =
        Assertions.assertThrows(
            UnauthorizedException.class, () -> kerberosAuthenticator.authenticateToken(null));
    Assertions.assertEquals("Empty token authorization header", e.getMessage());

    // case2 : Invalid token authorization header
    byte[] bytes = "Xx".getBytes(StandardCharsets.UTF_8);
    e =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> {
              kerberosAuthenticator.authenticateToken(bytes);
            });
    Assertions.assertEquals("Invalid token authorization header", e.getMessage());

    // case 3: Blank token found
    byte[] bytes2 = AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER.getBytes(StandardCharsets.UTF_8);
    e =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> {
              kerberosAuthenticator.authenticateToken(bytes2);
            });
    Assertions.assertEquals("Blank token found", e.getMessage());

    // case 4: Fail to validate the token
    String header = AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER + "xxxx";
    byte[] bytes3 = header.getBytes(StandardCharsets.UTF_8);
    e =
        Assertions.assertThrows(
            UnauthorizedException.class,
            () -> {
              kerberosAuthenticator.authenticateToken(bytes3);
            });
    Assertions.assertEquals("Fail to validate the token", e.getMessage());
  }

  @Test
  public void testAuthenticationNormally() throws Exception {
    KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();
    Config config = new Config(false) {};
    config.set(PRINCIPAL, KerberosTestUtils.getServerPrincipal());
    config.set(KEYTAB, KerberosTestUtils.getKeytabFile());
    initKeyTab();
    kerberosAuthenticator.initialize(config);
    String token =
        KerberosTestUtils.doAsClient(
            new Callable<String>() {
              @Override
              public String call() throws Exception {
                GSSManager gssManager = GSSManager.getInstance();
                GSSContext gssContext = null;
                try {
                  String servicePrincipal = KerberosTestUtils.getServerPrincipal();
                  Oid oid = KerberosUtils.NT_GSS_KRB5_PRINCIPAL_OID;
                  GSSName serviceName = gssManager.createName(servicePrincipal, oid);
                  oid = KerberosUtils.GSS_KRB5_MECH_OID;
                  gssContext =
                      gssManager.createContext(serviceName, oid, null, GSSContext.DEFAULT_LIFETIME);
                  gssContext.requestCredDeleg(true);
                  gssContext.requestMutualAuth(true);

                  byte[] inToken = new byte[0];
                  byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
                  return Base64.getEncoder().encodeToString(outToken);

                } finally {
                  if (gssContext != null) {
                    gssContext.dispose();
                  }
                }
              }
            });
    kerberosAuthenticator.authenticateToken(
        (AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER + token).getBytes(StandardCharsets.UTF_8));
  }

  private void initKeyTab() throws Exception {
    File keytabFile = new File(KerberosTestUtils.getKeytabFile());
    String clientPrincipal = removeRealm(KerberosTestUtils.getClientPrincipal());
    String serverPrincipal = removeRealm(KerberosTestUtils.getServerPrincipal());
    getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
  }

  private String removeRealm(String principal) {
    return principal.substring(0, principal.lastIndexOf("@"));
  }
}
