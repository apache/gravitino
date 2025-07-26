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

package org.apache.gravitino.client.integration.test.authorization;

import static org.apache.gravitino.server.authentication.KerberosConfig.KEYTAB;
import static org.apache.gravitino.server.authentication.KerberosConfig.PRINCIPAL;
import static org.apache.hadoop.minikdc.MiniKdc.MAX_TICKET_LIFETIME;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoVersion;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles;

public class KerberosOperationsIT extends BaseIT {

  private static final KerberosSecurityTestcase kdc =
      new KerberosSecurityTestcase() {
        @Override
        public void createMiniKdcConf() {
          super.createMiniKdcConf();
          getConf().setProperty(MAX_TICKET_LIFETIME, "5");
        }
      };

  private static final String keytabFile =
      new File(System.getProperty("test.dir", "target"), UUID.randomUUID().toString())
          .getAbsolutePath();

  // The following two keytab are needed both.
  private static final String serverPrincipal = "HTTP/localhost@EXAMPLE.COM";
  private static final String serverPrincipalWithAll = "HTTP/0.0.0.0@EXAMPLE.COM";

  private static final String clientPrincipal = "client@EXAMPLE.COM";

  public void setGravitinoAdminClient(GravitinoAdminClient client) {
    this.client = client;
  }

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    kdc.startMiniKdc();
    initKeyTab();

    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.KERBEROS.name().toLowerCase());
    configs.put(PRINCIPAL.getKey(), serverPrincipal);
    configs.put(KEYTAB.getKey(), keytabFile);
    configs.put("client.kerberos.principal", clientPrincipal);
    configs.put("client.kerberos.keytab", keytabFile);

    registerCustomConfigs(configs);

    super.startIntegrationTest();

    client =
        GravitinoAdminClient.builder(serverUri)
            .withKerberosAuth(
                KerberosTokenProvider.builder()
                    .withClientPrincipal(clientPrincipal)
                    .withKeyTabFile(new File(keytabFile))
                    .build())
            .build();
  }

  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    super.stopIntegrationTest();
    kdc.stopMiniKdc();
  }

  @Test
  public void testAuthenticationApi() throws Exception {
    GravitinoVersion gravitinoVersion = client.serverVersion();
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion.version());
    Assertions.assertFalse(gravitinoVersion.compileDate().isEmpty());

    if (testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      final String gitCommitId = readGitCommitIdFromGitFile();
      Assertions.assertEquals(gitCommitId, gravitinoVersion.gitCommit());
    }

    // Test to re-login with the keytab
    Uninterruptibles.sleepUninterruptibly(6, TimeUnit.SECONDS);
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion.version());
    Assertions.assertFalse(gravitinoVersion.compileDate().isEmpty());
  }

  private static void initKeyTab() throws Exception {
    File newKeytabFile = new File(keytabFile);
    String newClientPrincipal = removeRealm(clientPrincipal);
    String newServerPrincipal = removeRealm(serverPrincipal);
    String newServerPrincipalAll = removeRealm(serverPrincipalWithAll);
    kdc.getKdc()
        .createPrincipal(
            newKeytabFile, newClientPrincipal, newServerPrincipal, newServerPrincipalAll);
  }

  private static String removeRealm(String principal) {
    return principal.substring(0, principal.lastIndexOf("@"));
  }
}
