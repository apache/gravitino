/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.rest;

import static com.datastrato.gravitino.server.auth.KerberosConfig.KEYTAB;
import static com.datastrato.gravitino.server.auth.KerberosConfig.PRINCIPAL;

import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.client.GravitinoVersion;
import com.datastrato.gravitino.client.KerberosTokenProvider;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.KerberosProviderHelper;
import com.datastrato.gravitino.server.auth.KerberosConfig;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class KerberosOperationsIT extends AbstractIT {

  private static final KerberosSecurityTestcase kdc = new KerberosSecurityTestcase();

  private static final String keytabFile =
      new File(System.getProperty("test.dir", "target"), UUID.randomUUID().toString())
          .getAbsolutePath();

  private static final String serverPrincipal = "HTTP/localhost@EXAMPLE.COM";

  private static final String clientPrincipal = "client@EXAMPLE.COM";

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    kdc.startMiniKdc();
    initKeyTab();

    Map<String, String> configs = Maps.newHashMap();
    KerberosProviderHelper.setProvider(
        KerberosTokenProvider.builder()
            .withClientPrincipal(clientPrincipal)
            .withKeyTabFile(new File(keytabFile))
            .build());
    configs.put(
        KerberosConfig.AUTHENTICATOR.getKey(), AuthenticatorType.KERBEROS.name().toLowerCase());
    configs.put(PRINCIPAL.getKey(), serverPrincipal);
    configs.put(KEYTAB.getKey(), keytabFile);
    registerCustomConfigs(configs);

    AbstractIT.startIntegrationTest();
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    AbstractIT.stopIntegrationTest();
    kdc.stopMiniKdc();
  }

  @Test
  public void testAuthenticationApi() throws Exception {
    GravitinoVersion gravitinoVersion = client.getVersion();
    client.getVersion();
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion.version());
    Assertions.assertFalse(gravitinoVersion.compileDate().isEmpty());

    if (testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      final String gitCommitId = readGitCommitIdFromGitFile();
      Assertions.assertEquals(gitCommitId, gravitinoVersion.gitCommit());
    }
  }

  private static void initKeyTab() throws Exception {
    File newKeytabFile = new File(keytabFile);
    String newClientPrincipal = removeRealm(clientPrincipal);
    String newServerPrincipal = removeRealm(serverPrincipal);
    kdc.getKdc().createPrincipal(newKeytabFile, newClientPrincipal, newServerPrincipal);
  }

  private static String removeRealm(String principal) {
    return principal.substring(0, principal.lastIndexOf("@"));
  }
}
