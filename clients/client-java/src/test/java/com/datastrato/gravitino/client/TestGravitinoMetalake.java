/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.requests.CatalogCreateRequest;
import com.datastrato.gravitino.dto.requests.CatalogUpdateRequest;
import com.datastrato.gravitino.dto.requests.CatalogUpdatesRequest;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.RESTException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetalake extends TestBase {

  private static GravitinoMetaLake metalake;

  private static final String metalakeName = "test";

  private static final String provider = "test";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    metalake = createMetalake(client, metalakeName);
  }

  @Test
  public void testListCatalogs() throws JsonProcessingException {
    String path = "/api/metalakes/" + metalakeName + "/catalogs";

    NameIdentifier ident1 = NameIdentifier.of(metalakeName, "mock");
    NameIdentifier ident2 = NameIdentifier.of(metalakeName, "mock2");
    Namespace namespace = Namespace.of(metalakeName);

    EntityListResponse resp = new EntityListResponse(new NameIdentifier[] {ident1, ident2});
    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);
    NameIdentifier[] catalogs = metalake.listCatalogs(namespace);

    Assertions.assertEquals(2, catalogs.length);
    Assertions.assertEquals(ident1, catalogs[0]);
    Assertions.assertEquals(ident2, catalogs[1]);

    // Test return empty catalog list
    EntityListResponse resp1 = new EntityListResponse(new NameIdentifier[] {});
    buildMockResource(Method.GET, path, null, resp1, HttpStatus.SC_OK);
    NameIdentifier[] catalogs1 = metalake.listCatalogs(namespace);
    Assertions.assertEquals(0, catalogs1.length);

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex =
        Assertions.assertThrows(RuntimeException.class, () -> metalake.listCatalogs(namespace));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test return unparsed system error
    buildMockResource(Method.GET, path, null, "mock error", HttpStatus.SC_CONFLICT);
    Throwable ex1 =
        Assertions.assertThrows(RESTException.class, () -> metalake.listCatalogs(namespace));
    Assertions.assertTrue(ex1.getMessage().contains("Error code: " + HttpStatus.SC_CONFLICT));
  }

  @Test
  public void testLoadCatalog() throws JsonProcessingException {
    String catalogName = "mock";
    String path = "/api/metalakes/" + metalakeName + "/catalogs/" + catalogName;

    CatalogDTO mockCatalog =
        new CatalogDTO.Builder()
            .withName("mock")
            .withComment("comment")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogResponse resp = new CatalogResponse(mockCatalog);

    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));

    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals("comment", catalog.comment());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());

    // Test return not found
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchCatalogException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    NameIdentifier id = NameIdentifier.of(metalakeName, catalogName);
    Throwable ex =
        Assertions.assertThrows(NoSuchCatalogException.class, () -> metalake.loadCatalog(id));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test return unsupported catalog type
    CatalogDTO mockCatalog1 =
        new CatalogDTO.Builder()
            .withName("mock")
            .withComment("comment")
            .withType(Catalog.Type.MESSAGING)
            .withProvider("test")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogResponse resp1 = new CatalogResponse(mockCatalog1);
    buildMockResource(Method.GET, path, null, resp1, HttpStatus.SC_OK);
    Assertions.assertThrows(UnsupportedOperationException.class, () -> metalake.loadCatalog(id));

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 = Assertions.assertThrows(RuntimeException.class, () -> metalake.loadCatalog(id));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test return unparsed system error
    buildMockResource(Method.GET, path, null, "mock error", HttpStatus.SC_CONFLICT);
    Throwable ex2 = Assertions.assertThrows(RESTException.class, () -> metalake.loadCatalog(id));
    Assertions.assertTrue(ex2.getMessage().contains("Error code: " + HttpStatus.SC_CONFLICT));
  }

  @Test
  public void testCreateCatalog() throws JsonProcessingException {
    String catalogName = "mock";
    String path = "/api/metalakes/" + metalakeName + "/catalogs";

    CatalogDTO mockCatalog =
        new CatalogDTO.Builder()
            .withName(catalogName)
            .withComment("comment")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogCreateRequest req =
        new CatalogCreateRequest(
            catalogName, Catalog.Type.RELATIONAL, provider, "comment", Collections.emptyMap());
    CatalogResponse resp = new CatalogResponse(mockCatalog);
    buildMockResource(Method.POST, path, req, resp, HttpStatus.SC_OK);

    Catalog catalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            provider,
            "comment",
            Collections.emptyMap());
    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals("comment", catalog.comment());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());

    // Test return unsupported catalog type
    CatalogDTO mockCatalog1 =
        new CatalogDTO.Builder()
            .withName("mock")
            .withComment("comment")
            .withType(Catalog.Type.MESSAGING)
            .withProvider("test")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogCreateRequest req1 =
        new CatalogCreateRequest(
            catalogName, Catalog.Type.MESSAGING, provider, "comment", Collections.emptyMap());
    CatalogResponse resp1 = new CatalogResponse(mockCatalog1);
    buildMockResource(Method.POST, path, req1, resp1, HttpStatus.SC_OK);
    NameIdentifier id = NameIdentifier.of(metalakeName, catalogName);
    Map<String, String> emptyMap = Collections.emptyMap();

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> metalake.createCatalog(id, Catalog.Type.MESSAGING, provider, "comment", emptyMap));

    // Test return NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, path, req, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                metalake.createCatalog(id, Catalog.Type.RELATIONAL, provider, "comment", emptyMap));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test return CatalogAlreadyExistsException
    ErrorResponse errorResponse1 =
        ErrorResponse.alreadyExists(
            CatalogAlreadyExistsException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, path, req, errorResponse1, HttpStatus.SC_CONFLICT);
    Throwable ex1 =
        Assertions.assertThrows(
            CatalogAlreadyExistsException.class,
            () ->
                metalake.createCatalog(id, Catalog.Type.RELATIONAL, provider, "comment", emptyMap));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.POST, path, req, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex2 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                metalake.createCatalog(id, Catalog.Type.RELATIONAL, provider, "comment", emptyMap));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));
  }

  @Test
  public void testAlterCatalog() throws JsonProcessingException {
    String catalogName = "mock";
    String path = "/api/metalakes/" + metalakeName + "/catalogs/" + catalogName;

    CatalogDTO mockCatalog =
        new CatalogDTO.Builder()
            .withName("mock1")
            .withComment("comment1")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogResponse resp = new CatalogResponse(mockCatalog);

    CatalogChange change1 = CatalogChange.rename("mock1");
    CatalogChange change2 = CatalogChange.updateComment("comment1");
    List<CatalogUpdateRequest> reqs =
        Arrays.asList(change1, change2).stream()
            .map(DTOConverters::toCatalogUpdateRequest)
            .collect(Collectors.toList());
    CatalogUpdatesRequest updatesRequest = new CatalogUpdatesRequest(reqs);

    buildMockResource(Method.PUT, path, updatesRequest, resp, HttpStatus.SC_OK);
    NameIdentifier id = NameIdentifier.of(metalakeName, catalogName);
    Catalog catalog = metalake.alterCatalog(id, change1, change2);
    Assertions.assertEquals("mock1", catalog.name());
    Assertions.assertEquals("comment1", catalog.comment());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());

    // Test return NoSuchCatalogException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchCatalogException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, path, updatesRequest, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchCatalogException.class, () -> metalake.alterCatalog(id, change1, change2));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test return IllegalArgumentException
    ErrorResponse errorResponse1 = ErrorResponse.illegalArguments("mock error");
    buildMockResource(Method.PUT, path, updatesRequest, errorResponse1, HttpStatus.SC_BAD_REQUEST);
    Throwable ex1 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> metalake.alterCatalog(id, change1, change2));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(
        Method.PUT, path, updatesRequest, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex2 =
        Assertions.assertThrows(
            RuntimeException.class, () -> metalake.alterCatalog(id, change1, change2));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));
  }

  @Test
  public void testDropCatalog() throws JsonProcessingException {
    String catalogName = "mock";
    String path = "/api/metalakes/" + metalakeName + "/catalogs/" + catalogName;

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, path, null, resp, HttpStatus.SC_OK);
    boolean dropped = metalake.dropCatalog(NameIdentifier.of(metalakeName, catalogName));
    Assertions.assertTrue(dropped);

    // Test return false
    DropResponse resp1 = new DropResponse(false);
    buildMockResource(Method.DELETE, path, null, resp1, HttpStatus.SC_OK);
    boolean dropped1 = metalake.dropCatalog(NameIdentifier.of(metalakeName, catalogName));
    Assertions.assertFalse(dropped1);
  }

  static GravitinoMetaLake createMetalake(GravitinoClient client, String metalakeName)
      throws JsonProcessingException {
    MetalakeDTO mockMetalake =
        new MetalakeDTO.Builder()
            .withName(metalakeName)
            .withComment("comment")
            .withAudit(
                new AuditDTO.Builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeCreateRequest req =
        new MetalakeCreateRequest(metalakeName, "comment", Collections.emptyMap());
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.POST, "/api/metalakes", req, resp, HttpStatus.SC_OK);

    return client.createMetalake(
        NameIdentifier.parse(metalakeName), "comment", Collections.emptyMap());
  }
}
