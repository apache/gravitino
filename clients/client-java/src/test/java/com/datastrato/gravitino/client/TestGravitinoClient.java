/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
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

public class TestGravitinoClient extends TestBase {

  @Test
  public void testListMetalakes() throws JsonProcessingException {
    MetalakeDTO mockMetalake =
        new MetalakeDTO.Builder()
            .withName("mock")
            .withComment("comment")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeDTO mockMetalake1 =
        new MetalakeDTO.Builder()
            .withName("mock1")
            .withComment("comment1")
            .withAudit(
                new AuditDTO.Builder()
                    .withCreator("creator1")
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    MetalakeListResponse resp =
        new MetalakeListResponse(new MetalakeDTO[] {mockMetalake, mockMetalake1});
    buildMockResource(Method.GET, "/api/metalakes", null, resp, 200);
    GravitinoMetaLake[] metaLakes = client.listMetalakes();

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
    GravitinoMetaLake[] metaLakes1 = client.listMetalakes();
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
        new MetalakeDTO.Builder()
            .withName("mock")
            .withComment("comment")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.GET, "/api/metalakes/mock", null, resp, HttpStatus.SC_OK);
    NameIdentifier id = NameIdentifier.of("mock");
    GravitinoMetaLake metaLake = client.loadMetalake(id);
    Assertions.assertEquals("mock", metaLake.name());
    Assertions.assertEquals("comment", metaLake.comment());
    Assertions.assertEquals("creator", metaLake.auditInfo().creator());

    // Test return not found
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, "/api/metalakes/mock", null, errorResp, HttpStatus.SC_NOT_FOUND);
    Throwable excep =
        Assertions.assertThrows(NoSuchMetalakeException.class, () -> client.loadMetalake(id));
    Assertions.assertTrue(excep.getMessage().contains("mock error"));

    // Test illegal metalake name identifier
    NameIdentifier badName = NameIdentifier.parse("mock.mock");

    Throwable excep1 =
        Assertions.assertThrows(IllegalArgumentException.class, () -> client.loadMetalake(badName));
    Assertions.assertTrue(
        excep1.getMessage().contains("Metalake namespace must be non-null and empty"));

    // Test return unparsed system error
    buildMockResource(Method.GET, "/api/metalakes/mock", null, null, HttpStatus.SC_CONFLICT);
    Throwable excep2 = Assertions.assertThrows(RESTException.class, () -> client.loadMetalake(id));
    Assertions.assertTrue(excep2.getMessage().contains("Error code: " + HttpStatus.SC_CONFLICT));
  }

  @Test
  public void testCreateMetalake() throws JsonProcessingException {
    MetalakeDTO mockMetalake =
        new MetalakeDTO.Builder()
            .withName("mock")
            .withComment("comment")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    MetalakeCreateRequest req =
        new MetalakeCreateRequest("mock", "comment", Collections.emptyMap());
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.POST, "/api/metalakes", req, resp, HttpStatus.SC_OK);
    NameIdentifier id = NameIdentifier.parse("mock");
    GravitinoMetaLake metaLake = client.createMetalake(id, "comment", Collections.emptyMap());
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
            () -> client.createMetalake(id, "comment", emptyMap));
    Assertions.assertTrue(excep.getMessage().contains("mock error"));

    // Test return unparsed system error
    buildMockResource(Method.POST, "/api/metalakes", req, null, HttpStatus.SC_CONFLICT);
    Throwable excep1 =
        Assertions.assertThrows(
            RESTException.class, () -> client.createMetalake(id, "comment", emptyMap));
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
        new MetalakeDTO.Builder()
            .withName("newName")
            .withComment("newComment")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);

    buildMockResource(Method.PUT, "/api/metalakes/mock", req, resp, HttpStatus.SC_OK);
    NameIdentifier id = NameIdentifier.of("mock");
    GravitinoMetaLake metaLake = client.alterMetalake(id, changes);
    Assertions.assertEquals("newName", metaLake.name());
    Assertions.assertEquals("newComment", metaLake.comment());
    Assertions.assertEquals("creator", metaLake.auditInfo().creator());

    // Test return not found
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, "/api/metalakes/mock", req, errorResp, HttpStatus.SC_NOT_FOUND);
    Throwable excep =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> client.alterMetalake(id, changes));
    Assertions.assertTrue(excep.getMessage().contains("mock error"));

    // Test illegal argument
    NameIdentifier id2 = NameIdentifier.parse("mock.mock");
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
    Assertions.assertTrue(client.dropMetalake(NameIdentifier.of("mock")));

    DropResponse resp1 = new DropResponse(false);
    buildMockResource(Method.DELETE, "/api/metalakes/mock", null, resp1, HttpStatus.SC_OK);
    Assertions.assertFalse(client.dropMetalake(NameIdentifier.of("mock")));

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(
        Method.DELETE, "/api/metalakes/mock", null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Assertions.assertFalse(client.dropMetalake(NameIdentifier.of("mock")));

    // Test illegal metalake name identifier
    NameIdentifier id = NameIdentifier.parse("mock.mock");
    Throwable excep1 =
        Assertions.assertThrows(IllegalArgumentException.class, () -> client.dropMetalake(id));
    Assertions.assertTrue(
        excep1.getMessage().contains("Metalake namespace must be non-null and empty"));
  }
}
