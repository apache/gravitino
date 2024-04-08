/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.file.FilesetDTO;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.FilesetResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.datastrato.gravitino.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;

public abstract class GravitinoMockServerBase {
  private static final ObjectMapper MAPPER = JsonUtils.objectMapper();
  private static ClientAndServer mockServer;
  private static final String MOCK_SERVER_HOST = "http://127.0.0.1:";
  private static int port;
  protected static final String metalakeName = "metalake_1";
  protected static final String catalogName = "fileset_catalog_1";
  protected static final String schemaName = "schema_1";
  protected static final String managedFilesetName = "managed_fileset";
  protected static final String externalFilesetName = "external_fileset";
  protected static final String provider = "test";

  @BeforeAll
  public static void setup() {
    mockServer = ClientAndServer.startClientAndServer(0);
    port = mockServer.getLocalPort();
  }

  @AfterEach
  public void reset() {
    mockServer.reset();
  }

  @AfterAll
  public static void tearDown() {
    mockServer.stop();
  }

  public static String serverUri() {
    return String.format("%s%d", MOCK_SERVER_HOST, port);
  }

  protected static <T, R> void buildMockResource(
      Method method,
      String path,
      Map<String, String> queryParams,
      T reqBody,
      R respBody,
      int statusCode)
      throws com.datastrato.gravitino.shaded.com.fasterxml.jackson.core.JsonProcessingException {
    List<Parameter> parameters =
        queryParams.entrySet().stream()
            .map(kv -> new Parameter(kv.getKey(), kv.getValue()))
            .collect(Collectors.toList());

    HttpRequest mockRequest =
        HttpRequest.request(path).withMethod(method.name()).withQueryStringParameters(parameters);
    if (reqBody != null) {
      String reqJson = MAPPER.writeValueAsString(reqBody);
      mockRequest = mockRequest.withBody(reqJson);
    }

    HttpResponse mockResponse = HttpResponse.response().withStatusCode(statusCode);
    if (respBody != null) {
      String respJson = MAPPER.writeValueAsString(respBody);
      mockResponse = mockResponse.withBody(respJson);
    }

    mockServer.when(mockRequest, Times.unlimited()).respond(mockResponse);
  }

  protected static <T, R> void buildMockResource(
      Method method, String path, T reqBody, R respBody, int statusCode)
      throws com.datastrato.gravitino.shaded.com.fasterxml.jackson.core.JsonProcessingException {
    buildMockResource(method, path, Collections.emptyMap(), reqBody, respBody, statusCode);
  }

  protected static void mockMetalakeDTO(String name, String comment) {
    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName(name)
            .withComment(comment)
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    try {
      buildMockResource(Method.GET, "/api/metalakes/" + metalakeName, null, resp, HttpStatus.SC_OK);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  protected static void mockCatalogDTO(String catalogName, String provider, String comment) {
    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName(catalogName)
            .withType(CatalogDTO.Type.FILESET)
            .withProvider(provider)
            .withComment(comment)
            .withProperties(ImmutableMap.of("k1", "k2"))
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    CatalogResponse catalogResponse = new CatalogResponse(mockCatalog);
    try {
      buildMockResource(
          Method.GET,
          "/api/metalakes/" + metalakeName + "/catalogs/" + catalogName,
          null,
          catalogResponse,
          SC_OK);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  protected static void mockFilesetDTO(
      String metalakeName,
      String catalogName,
      String schemaName,
      String filesetName,
      Fileset.Type type,
      String location) {
    NameIdentifier fileset = NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName);
    String filesetPath =
        String.format(
            "/api/metalakes/%s/catalogs/%s/schemas/%s/filesets/%s",
            metalakeName, catalogName, schemaName, filesetName);
    FilesetDTO mockFileset =
        FilesetDTO.builder()
            .name(fileset.name())
            .type(type)
            .storageLocation(location)
            .comment("comment")
            .properties(ImmutableMap.of("k1", "v1"))
            .audit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    FilesetResponse filesetResponse = new FilesetResponse(mockFileset);
    try {
      buildMockResource(Method.GET, filesetPath, null, filesetResponse, SC_OK);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static ClientAndServer mockServer() {
    return mockServer;
  }
}
