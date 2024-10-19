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

import static org.apache.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;
import static org.apache.gravitino.server.authentication.KerberosConfig.KEYTAB;
import static org.apache.gravitino.server.authentication.KerberosConfig.PRINCIPAL;
import static org.apache.hadoop.minikdc.MiniKdc.MAX_TICKET_LIFETIME;

import com.google.common.collect.Maps;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Date;
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
import org.apache.gravitino.integration.test.util.OAuthMockDataProvider;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles;

@Tag("gravitino-docker-test")
public class MultiAuthOperationsIT extends BaseIT {
  private static final KerberosSecurityTestcase kdc =
      new KerberosSecurityTestcase() {
        @Override
        public void createMiniKdcConf() {
          super.createMiniKdcConf();
          getConf().setProperty(MAX_TICKET_LIFETIME, "5");
        }
      };
  private static final String clientPrincipal = "client@EXAMPLE.COM";
  private static final String keytabFile =
      new File(System.getProperty("test.dir", "target"), UUID.randomUUID().toString())
          .getAbsolutePath();

  private static GravitinoAdminClient oauthClient;
  private static GravitinoAdminClient kerberosClient;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(
        Configs.AUTHENTICATORS.getKey(),
        String.join(
            ",",
            AuthenticatorType.KERBEROS.name().toLowerCase(),
            AuthenticatorType.OAUTH.name().toLowerCase()));

    configOAuth(configs);
    configKerberos(configs);

    registerCustomConfigs(configs);
    super.startIntegrationTest();

    oauthClient =
        GravitinoAdminClient.builder(serverUri)
            .withOAuth(OAuthMockDataProvider.getInstance())
            .build();

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);
    serverUri = "http://localhost:" + jettyServerConfig.getHttpPort();
    kerberosClient =
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
  public void testMultiAuthenticationSuccess() throws Exception {
    GravitinoVersion gravitinoVersion1 = oauthClient.serverVersion();
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion1.version());
    Assertions.assertFalse(gravitinoVersion1.compileDate().isEmpty());
    if (testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      final String gitCommitId = readGitCommitIdFromGitFile();
      Assertions.assertEquals(gitCommitId, gravitinoVersion1.gitCommit());
    }

    KerberosOperationsIT kerberosOperationsIT = new KerberosOperationsIT();
    kerberosOperationsIT.setGravitinoAdminClient(client);
    kerberosOperationsIT.testAuthenticationApi();

    GravitinoVersion gravitinoVersion2 = kerberosClient.serverVersion();
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion2.version());
    Assertions.assertFalse(gravitinoVersion2.compileDate().isEmpty());

    if (testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      final String gitCommitId = readGitCommitIdFromGitFile();
      Assertions.assertEquals(gitCommitId, gravitinoVersion2.gitCommit());
    }

    // Test to re-login with the keytab
    Uninterruptibles.sleepUninterruptibly(6, TimeUnit.SECONDS);
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion2.version());
    Assertions.assertFalse(gravitinoVersion2.compileDate().isEmpty());
  }

  @SuppressWarnings("JavaUtilDate")
  private static void configOAuth(Map<String, String> configs) {
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
    String publicKey =
        new String(
            Base64.getEncoder().encode(keyPair.getPublic().getEncoded()), StandardCharsets.UTF_8);

    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), "service1");
    configs.put(OAuthConfig.DEFAULT_SIGN_KEY.getKey(), publicKey);
    configs.put(OAuthConfig.ALLOW_SKEW_SECONDS.getKey(), "6");
    configs.put(OAuthConfig.DEFAULT_SERVER_URI.getKey(), "test");
    configs.put(OAuthConfig.DEFAULT_TOKEN_PATH.getKey(), "test");

    String token =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 1000 * 1000))
            .setAudience("service1")
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    OAuthMockDataProvider mockDataProvider = OAuthMockDataProvider.getInstance();
    mockDataProvider.setTokenData(token.getBytes(StandardCharsets.UTF_8));
  }

  private static void configKerberos(Map<String, String> configs) throws Exception {
    kdc.startMiniKdc();

    final String serverPrincipal = "HTTP/localhost@EXAMPLE.COM";
    final String serverPrincipalWithAll = "HTTP/0.0.0.0@EXAMPLE.COM";

    File newKeytabFile = new File(keytabFile);
    String newClientPrincipal = removeRealm(clientPrincipal);
    String newServerPrincipal = removeRealm(serverPrincipal);
    String newServerPrincipalAll = removeRealm(serverPrincipalWithAll);
    kdc.getKdc()
        .createPrincipal(
            newKeytabFile, newClientPrincipal, newServerPrincipal, newServerPrincipalAll);

    configs.put(PRINCIPAL.getKey(), serverPrincipal);
    configs.put(KEYTAB.getKey(), keytabFile);
    configs.put("client.kerberos.principal", clientPrincipal);
    configs.put("client.kerberos.keytab", keytabFile);
  }

  private static String removeRealm(String principal) {
    return principal.substring(0, principal.lastIndexOf("@"));
  }
}
