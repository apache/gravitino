/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Version;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.VersionDTO;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.responses.VersionResponse;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.RESTException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

public class TestGravitinoClient extends TestBase {

  @Test
  public void testListMetalakes() throws JsonProcessingException {
    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName("mock")
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeDTO mockMetalake1 =
        MetalakeDTO.builder()
            .withName("mock1")
            .withComment("comment1")
            .withAudit(
                AuditDTO.builder().withCreator("creator1").withCreateTime(Instant.now()).build())
            .build();

    MetalakeListResponse resp =
        new MetalakeListResponse(new MetalakeDTO[] {mockMetalake, mockMetalake1});
    buildMockResource(Method.GET, "/api/metalakes", null, resp, 200);
    GravitinoMetalake[] metaLakes = client.listMetalakes();

    Assertions.assertEquals(2, metaLakes.length);
    Assertions.assertEquals("mock", metaLakes[0].name());
    Assertions.assertEquals("comment", metaLakes[0].comment());
    Assertions.assertEquals("creator", metaLakes[0].auditInfo().creator());

    Assertions.assertEquals("mock1", metaLakes[1].name());
    Assertions.assertEquals("comment1", metaLakes[1].comment());
    Assertions.assertEquals("creator1", metaLakes[1].auditInfo().creator());

    // Test return empty metalake list
    MetalakeListResponse resp1 = new MetalakeListResponse(new MetalakeDTO[] {});
    buildMockResource(Method.GET, "/api/metalakes", null, resp1, HttpStatus.SC_OK);
    GravitinoMetalake[] metaLakes1 = client.listMetalakes();
    Assertions.assertEquals(0, metaLakes1.length);

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(
        Method.GET, "/api/metalakes", null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable excep = Assertions.assertThrows(RuntimeException.class, () -> client.listMetalakes());
    Assertions.assertTrue(excep.getMessage().contains("mock error"));

    // Test return unparsed system error
    buildMockResource(Method.GET, "/api/metalakes", null, null, HttpStatus.SC_CONFLICT);
    Throwable excep1 = Assertions.assertThrows(RESTException.class, () -> client.listMetalakes());
    Assertions.assertTrue(excep1.getMessage().contains("Error code: " + HttpStatus.SC_CONFLICT));
  }

  @Test
  public void testLoadMetalake() throws JsonProcessingException {
    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName("mock")
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.GET, "/api/metalakes/mock", null, resp, HttpStatus.SC_OK);
    NameIdentifier id = NameIdentifier.of("mock");
    GravitinoMetalake metaLake = client.loadMetalake(id.name());
    Assertions.assertEquals("mock", metaLake.name());
    Assertions.assertEquals("comment", metaLake.comment());
    Assertions.assertEquals("creator", metaLake.auditInfo().creator());

    // Test return not found
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, "/api/metalakes/mock", null, errorResp, HttpStatus.SC_NOT_FOUND);
    Throwable excep =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> client.loadMetalake(id.name()));
    Assertions.assertTrue(excep.getMessage().contains("mock error"));

    // Test illegal metalake name identifier
    String badName = "mock.mock";

    Throwable excep1 =
        Assertions.assertThrows(
            IllegalNamespaceException.class, () -> client.loadMetalake(badName));
    Assertions.assertTrue(
        excep1.getMessage().contains("Metalake namespace must be non-null and empty"));

    // Test return unparsed system error
    buildMockResource(Method.GET, "/api/metalakes/mock", null, null, HttpStatus.SC_CONFLICT);
    Throwable excep2 =
        Assertions.assertThrows(RESTException.class, () -> client.loadMetalake(id.name()));
    Assertions.assertTrue(excep2.getMessage().contains("Error code: " + HttpStatus.SC_CONFLICT));
  }

  @Test
  public void testCreateMetalake() throws JsonProcessingException {
    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName("mock")
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    MetalakeCreateRequest req =
        new MetalakeCreateRequest("mock", "comment", Collections.emptyMap());
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.POST, "/api/metalakes", req, resp, HttpStatus.SC_OK);
    NameIdentifier id = NameIdentifier.parse("mock");
    GravitinoMetalake metaLake =
        client.createMetalake(id.name(), "comment", Collections.emptyMap());
    Map<String, String> emptyMap = Collections.emptyMap();

    Assertions.assertEquals("mock", metaLake.name());
    Assertions.assertEquals("comment", metaLake.comment());
    Assertions.assertEquals("creator", metaLake.auditInfo().creator());

    // Test metalake name already exists
    ErrorResponse errorResp =
        ErrorResponse.alreadyExists(
            MetalakeAlreadyExistsException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, "/api/metalakes", req, errorResp, HttpStatus.SC_CONFLICT);
    Throwable excep =
        Assertions.assertThrows(
            MetalakeAlreadyExistsException.class,
            () -> client.createMetalake(id.name(), "comment", emptyMap));
    Assertions.assertTrue(excep.getMessage().contains("mock error"));

    // Test return unparsed system error
    buildMockResource(Method.POST, "/api/metalakes", req, null, HttpStatus.SC_CONFLICT);
    Throwable excep1 =
        Assertions.assertThrows(
            RESTException.class, () -> client.createMetalake(id.name(), "comment", emptyMap));
    Assertions.assertTrue(excep1.getMessage().contains("Error code: " + HttpStatus.SC_CONFLICT));
  }

  @Test
  public void testAlterMetalake() throws JsonProcessingException {
    MetalakeChange[] changes =
        new MetalakeChange[] {
          MetalakeChange.rename("newName"), MetalakeChange.updateComment("newComment")
        };
    MetalakeUpdatesRequest req =
        new MetalakeUpdatesRequest(
            Arrays.stream(changes)
                .map(DTOConverters::toMetalakeUpdateRequest)
                .collect(Collectors.toList()));

    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName("newName")
            .withComment("newComment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);

    buildMockResource(Method.PUT, "/api/metalakes/mock", req, resp, HttpStatus.SC_OK);
    NameIdentifier id = NameIdentifier.of("mock");
    GravitinoMetalake metaLake = client.alterMetalake(id.name(), changes);
    Assertions.assertEquals("newName", metaLake.name());
    Assertions.assertEquals("newComment", metaLake.comment());
    Assertions.assertEquals("creator", metaLake.auditInfo().creator());

    // Test return not found
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, "/api/metalakes/mock", req, errorResp, HttpStatus.SC_NOT_FOUND);
    Throwable excep =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> client.alterMetalake(id.name(), changes));
    Assertions.assertTrue(excep.getMessage().contains("mock error"));

    // Test illegal argument
    String id2 = "mock.mock";
    Throwable excep1 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> client.alterMetalake(id2, changes));
    Assertions.assertTrue(
        excep1.getMessage().contains("Metalake namespace must be non-null and empty"));
  }

  @Test
  public void testDropMetalake() throws JsonProcessingException {
    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, "/api/metalakes/mock", null, resp, HttpStatus.SC_OK);
    Assertions.assertTrue(client.dropMetalake("mock"), "metalake should be dropped");

    DropResponse resp1 = new DropResponse(false);
    buildMockResource(Method.DELETE, "/api/metalakes/mock", null, resp1, HttpStatus.SC_OK);
    Assertions.assertFalse(client.dropMetalake("mock"), "metalake should be non-existent");

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(
        Method.DELETE, "/api/metalakes/mock", null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable excep =
        Assertions.assertThrows(RuntimeException.class, () -> client.dropMetalake("mock"));
    Assertions.assertTrue(excep.getMessage().contains("mock error"));

    // Test illegal metalake name identifier
    String badName = "mock.mock";
    Throwable excep1 =
        Assertions.assertThrows(IllegalArgumentException.class, () -> client.dropMetalake(badName));
    Assertions.assertTrue(
        excep1.getMessage().contains("Metalake namespace must be non-null and empty"));
  }

  @Test
  public void testGetServerVersion() throws JsonProcessingException {
    String version = "0.1.3";
    String date = "2024-01-03 12:28:33";
    String commitId = "6ef1f9d";

    VersionResponse resp = new VersionResponse(new VersionDTO(version, date, commitId));
    buildMockResource(Method.GET, "/api/version", null, resp, HttpStatus.SC_OK);
    GravitinoVersion gravitinoVersion = client.serverVersion();

    Assertions.assertEquals(version, gravitinoVersion.version());
    Assertions.assertEquals(date, gravitinoVersion.compileDate());
    Assertions.assertEquals(commitId, gravitinoVersion.gitCommit());
  }

  @Test
  public void testGetClientVersion() {
    GravitinoVersion version = client.clientVersion();
    Version.VersionInfo currentVersion = Version.getCurrentVersion();

    Assertions.assertEquals(currentVersion.version, version.version());
    Assertions.assertEquals(currentVersion.compileDate, version.compileDate());
    Assertions.assertEquals(currentVersion.gitCommit, version.gitCommit());
  }

  @Test
  public void testCheckVersionFailed() throws JsonProcessingException {
    String version = "0.1.1";
    String date = "2024-01-03 12:28:33";
    String commitId = "6ef1f9d";

    VersionResponse resp = new VersionResponse(new VersionDTO(version, date, commitId));
    buildMockResource(Method.GET, "/api/version", null, resp, HttpStatus.SC_OK);

    // check the client version is greater than server version
    Assertions.assertThrows(GravitinoRuntimeException.class, () -> client.checkVersion());
  }

  @Test
  public void testCheckVersionSuccess() throws JsonProcessingException {
    VersionResponse resp = new VersionResponse(Version.getCurrentVersionDTO());
    buildMockResource(Method.GET, "/api/version", null, resp, HttpStatus.SC_OK);

    // check the client version is equal to server version
    Assertions.assertDoesNotThrow(() -> client.checkVersion());

    String version = "100.1.1-SNAPSHOT";
    String date = "2024-01-03 12:28:33";
    String commitId = "6ef1f9d";

    resp = new VersionResponse(new VersionDTO(version, date, commitId));
    buildMockResource(Method.GET, "/api/version", null, resp, HttpStatus.SC_OK);

    // check the client version is less than server version
    Assertions.assertDoesNotThrow(() -> client.checkVersion());
  }

  @Test
  public void testUnusedDTOAttribute() throws JsonProcessingException {
    VersionResponse resp = new VersionResponse(Version.getCurrentVersionDTO());

    HttpRequest mockRequest = HttpRequest.request("/api/version").withMethod(Method.GET.name());
    HttpResponse mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_OK);
    String respJson = MAPPER.writeValueAsString(resp);

    // add unused attribute for version DTO
    respJson = respJson.replace("\"gitCommit\"", "\"unused_key\":\"unused_value\", \"gitCommit\"");
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(mockRequest, Times.exactly(1)).respond(mockResponse);

    Assertions.assertDoesNotThrow(
        () -> {
          GravitinoVersion version = client.serverVersion();
          Version.VersionInfo currentVersion = Version.getCurrentVersion();
          Assertions.assertEquals(currentVersion.version, version.version());
          Assertions.assertEquals(currentVersion.compileDate, version.compileDate());
          Assertions.assertEquals(currentVersion.gitCommit, version.gitCommit());
        });
  }
}
