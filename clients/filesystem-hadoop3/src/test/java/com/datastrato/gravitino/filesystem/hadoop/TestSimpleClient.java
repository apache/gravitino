/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.json.JsonUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

public class TestSimpleClient extends TestGvfsBase {
  @BeforeAll
  public static void setup() {
    TestGvfsBase.setup();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
  }

  @Test
  public void testSimpleAuthToken() throws IOException {
    // mock load metalake with expired token
    String testMetalake = "test_token";
    HttpRequest mockRequest =
        HttpRequest.request("/api/metalakes/" + testMetalake)
            .withMethod(Method.GET.name())
            .withQueryStringParameters(Collections.emptyMap());

    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName(testMetalake)
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);

    AtomicReference<String> actualTokenValue = new AtomicReference<>();
    GravitinoMockServerBase.mockServer()
        .when(mockRequest, Times.unlimited())
        .respond(
            httpRequest -> {
              List<Header> headers = httpRequest.getHeaders().getEntries();
              for (Header header : headers) {
                if (header.getName().equalsIgnoreCase("Authorization")) {
                  actualTokenValue.set(header.getValues().get(0).getValue());
                }
              }
              HttpResponse mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_OK);
              String respJson = JsonUtils.objectMapper().writeValueAsString(resp);
              mockResponse = mockResponse.withBody(respJson);
              return mockResponse;
            });

    // set the user for simple authentication
    String user = "test";
    System.setProperty("user.name", user);

    Path newPath = new Path(managedFilesetPath.toString().replace(metalakeName, testMetalake));

    Configuration config1 = new Configuration(conf);
    config1.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, testMetalake);
    newPath.getFileSystem(config1);

    String userInformation = user + ":dummy";
    assertEquals(
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + new String(
                Base64.getEncoder().encode(userInformation.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8),
        actualTokenValue.get());
  }
}
