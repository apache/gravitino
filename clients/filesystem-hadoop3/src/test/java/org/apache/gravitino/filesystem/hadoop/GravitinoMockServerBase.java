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
package org.apache.gravitino.filesystem.hadoop;

import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Version;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.FilesetResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.responses.VersionResponse;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.json.JsonUtils;
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
  private static final String MOCK_SERVER_HOST = "http://localhost:";
  private static int port;
  protected static final String metalakeName = "metalake_1";
  protected static final String catalogName = "fileset_catalog_1";
  protected static final String schemaName = "schema_1";
  protected static final String provider = "test";

  @BeforeAll
  public static void setup() {
    mockServer = ClientAndServer.startClientAndServer(0);
    port = mockServer.getLocalPort();
    mockAPIVersion();
  }

  @AfterEach
  public void reset() {
    mockServer.reset();
    mockAPIVersion();
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
      throws JsonProcessingException {
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
      throws JsonProcessingException {
    buildMockResource(method, path, Collections.emptyMap(), reqBody, respBody, statusCode);
  }

  protected static void mockAPIVersion() {
    try {
      buildMockResource(
          Method.GET,
          "/api/version",
          null,
          new VersionResponse(Version.getCurrentVersionDTO()),
          HttpStatus.SC_OK);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
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
