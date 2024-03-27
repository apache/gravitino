/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop3;

import static org.mockito.ArgumentMatchers.any;

import com.datastrato.gravitino.dto.responses.OAuth2TokenResponse;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.security.KeyPair;
import java.util.Date;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpResponse;

public abstract class Oauth2MockServerBase {
  private static ClientAndServer mockServer;
  private static final String MOCK_SERVER_HOST = "http://127.0.0.1:";
  private static int port;

  @SuppressWarnings("JavaUtilDate")
  @BeforeAll
  public static void setup() {
    mockServer = ClientAndServer.startClientAndServer(0);
    port = mockServer.getLocalPort();
    KeyPair keyPair = Keys.keyPairFor(SignatureAlgorithm.RS256);
    String accessToken =
        Jwts.builder()
            .setSubject("gravitino")
            .setExpiration(new Date(System.currentTimeMillis() + 10000))
            .setAudience("service1")
            .signWith(keyPair.getPrivate(), SignatureAlgorithm.RS256)
            .compact();
    OAuth2TokenResponse response =
        new OAuth2TokenResponse(accessToken, "2", "bearer", 1, "test", null);
    try {
      String respJson = JsonUtils.objectMapper().writeValueAsString(response);
      HttpResponse mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_OK);
      mockResponse = mockResponse.withBody(respJson);
      mockServer.when(any(), Times.unlimited()).respond(mockResponse);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  public static void tearDown() {
    mockServer.stop();
  }

  public static String serverUri() {
    return String.format("%s%d", MOCK_SERVER_HOST, port);
  }
}
