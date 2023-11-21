/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.rest;

import static com.datastrato.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.client.ErrorHandlers;
import com.datastrato.gravitino.client.HTTPClient;
import com.datastrato.gravitino.dto.responses.VersionResponse;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.CommandExecutor;
import com.datastrato.gravitino.integration.test.util.ProcessData;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.server.auth.OAuthConfig;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.google.common.collect.Maps;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.security.KeyPair;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AuthenticationOperationsIT extends AbstractIT {

  private static final KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
  private static final String publicKey =
      new String(Base64.getEncoder().encode(keyPair.getPublic().getEncoded()));

  private static String token;

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    token =
        AuthConstants.AUTHORIZATION_BEARER_HEADER
            + Jwts.builder()
                .setSubject("gravitino")
                .setExpiration(new Date(System.currentTimeMillis() + 1000 * 1000))
                .setAudience("service1")
                .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
                .compact();
    configs.put(OAuthConfig.AUTHENTICATOR.getKey(), AuthenticatorType.OAUTH.name().toLowerCase());
    configs.put(OAuthConfig.SERVICE_AUDIENCE.getKey(), "service1");
    configs.put(OAuthConfig.DEFAULT_SIGN_KEY.getKey(), publicKey);
    configs.put(OAuthConfig.ALLOW_SKEW_SECONDS.getKey(), "6");
    configs.put(AuthConstants.HTTP_HEADER_AUTHORIZATION, token);
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();
  }

  @Test
  public void testAuthenticationApi() throws Exception {
    Object ret =
        CommandExecutor.executeCommandLocalHost(
            "git rev-parse HEAD", false, ProcessData.TypesOfData.OUTPUT);
    String gitCommitId = ret.toString().replace("\n", "");

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);

    String uri = "http://" + jettyServerConfig.getHost() + ":" + jettyServerConfig.getHttpPort();

    try (HTTPClient restClient =
        HTTPClient.builder(Collections.emptyMap())
            .uri(uri)
            .withObjectMapper(JsonUtils.objectMapper())
            .build(); ) {
      Map<String, String> headers = Maps.newHashMap();
      headers.put(AuthConstants.HTTP_HEADER_AUTHORIZATION, token);
      VersionResponse resp =
          restClient.get(
              "api/version", VersionResponse.class, headers, ErrorHandlers.restErrorHandler());
      resp.validate();
      String version = resp.getVersion().version();
      String compileDate = resp.getVersion().compileDate();
      String respGitCommit = resp.getVersion().gitCommit();
      Assertions.assertEquals(System.getenv("PROJECT_VERSION"), version);
      Assertions.assertFalse(compileDate.isEmpty());
      Assertions.assertEquals(gitCommitId, respGitCommit);
    }
  }
}
