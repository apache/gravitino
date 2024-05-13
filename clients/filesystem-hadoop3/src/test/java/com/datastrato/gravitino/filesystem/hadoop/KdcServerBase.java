/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.filesystem.hadoop;

import java.io.File;
import java.util.UUID;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;

public class KdcServerBase extends KerberosSecurityTestcase {
  private static final KerberosSecurityTestcase INSTANCE = new KerberosSecurityTestcase();
  private static final String CLIENT_PRINCIPAL = "client@EXAMPLE.COM";
  private static final String SERVER_PRINCIPAL = "HTTP/localhost@EXAMPLE.COM";
  private static final String KEYTAB_FILE =
      new File(System.getProperty("test.dir", "target"), UUID.randomUUID().toString())
          .getAbsolutePath();

  static {
    try {
      INSTANCE.startMiniKdc();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private KdcServerBase() {}

  public static void stopKdc() {
    INSTANCE.stopMiniKdc();
  }

  public static void initKeyTab() throws Exception {
    File keytabFile = new File(KEYTAB_FILE);
    String clientPrincipal = removeRealm(CLIENT_PRINCIPAL);
    String serverPrincipal = removeRealm(SERVER_PRINCIPAL);
    INSTANCE.getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
  }

  private static String removeRealm(String principal) {
    return principal.substring(0, principal.lastIndexOf("@"));
  }

  public static String getServerPrincipal() {
    return SERVER_PRINCIPAL;
  }

  public static String getClientPrincipal() {
    return CLIENT_PRINCIPAL;
  }

  public static String getKeytabFile() {
    return KEYTAB_FILE;
  }
}
