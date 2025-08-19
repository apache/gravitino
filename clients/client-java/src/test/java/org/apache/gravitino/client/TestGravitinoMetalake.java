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
package org.apache.gravitino.client;

import static org.apache.gravitino.dto.util.DTOConverters.fromDTO;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.dto.policy.PolicyDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.dto.requests.MetalakeCreateRequest;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdatesRequest;
import org.apache.gravitino.dto.requests.TagCreateRequest;
import org.apache.gravitino.dto.requests.TagUpdateRequest;
import org.apache.gravitino.dto.requests.TagUpdatesRequest;
import org.apache.gravitino.dto.responses.CatalogListResponse;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
import org.apache.gravitino.dto.responses.PolicyResponse;
import org.apache.gravitino.dto.responses.TagListResponse;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetalake extends TestBase {

  private static final String metalakeName = "test";

  private static final String provider = "test";

  private static final Instant testStartTime = Instant.now();

  protected static GravitinoClient gravitinoClient;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    createMetalake(client, metalakeName);

    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName(metalakeName)
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.GET, "/api/metalakes/" + metalakeName, null, resp, HttpStatus.SC_OK);

    gravitinoClient =
        GravitinoClient.builder("http://127.0.0.1:" + mockServer.getLocalPort())
            .withMetalake(metalakeName)
            .withVersionCheckDisabled()
            .build();
  }

  @AfterAll
  public static void tearDown() {
    TestBase.tearDown();
    gravitinoClient.close();
  }

  @Test
  public void testListCatalogs() throws JsonProcessingException {
    String path = "/api/metalakes/" + metalakeName + "/catalogs";

    NameIdentifier ident1 = NameIdentifier.of(metalakeName, "mock");
    NameIdentifier ident2 = NameIdentifier.of(metalakeName, "mock2");

    EntityListResponse resp = new EntityListResponse(new NameIdentifier[] {ident1, ident2});
    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);
    String[] catalogs = gravitinoClient.listCatalogs();

    Assertions.assertEquals(2, catalogs.length);
    Assertions.assertEquals(ident1.name(), catalogs[0]);
    Assertions.assertEquals(ident2.name(), catalogs[1]);

    // Test return empty catalog list
    EntityListResponse resp1 = new EntityListResponse(new NameIdentifier[] {});
    buildMockResource(Method.GET, path, null, resp1, HttpStatus.SC_OK);
    String[] catalogs1 = gravitinoClient.listCatalogs();
    Assertions.assertEquals(0, catalogs1.length);

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex =
        Assertions.assertThrows(RuntimeException.class, () -> gravitinoClient.listCatalogs());
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test return unparsed system error
    buildMockResource(Method.GET, path, null, "mock error", HttpStatus.SC_CONFLICT);
    Throwable ex1 =
        Assertions.assertThrows(RESTException.class, () -> gravitinoClient.listCatalogs());
    Assertions.assertTrue(ex1.getMessage().contains("Error code: " + HttpStatus.SC_CONFLICT));
  }

  @Test
  public void testListCatalogsInfo() throws JsonProcessingException {
    String path = "/api/metalakes/" + metalakeName + "/catalogs";
    Map<String, String> params = Collections.singletonMap("details", "true");

    CatalogDTO mockCatalog1 =
        CatalogDTO.builder()
            .withName("mock")
            .withComment("comment")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogDTO mockCatalog2 =
        CatalogDTO.builder()
            .withName("mock2")
            .withComment("comment2")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogListResponse resp =
        new CatalogListResponse(new CatalogDTO[] {mockCatalog1, mockCatalog2});
    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);

    Catalog[] catalogs = gravitinoClient.listCatalogsInfo();
    Assertions.assertEquals(2, catalogs.length);
    Assertions.assertEquals("mock", catalogs[0].name());
    Assertions.assertEquals("comment", catalogs[0].comment());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogs[0].type());
    Assertions.assertEquals("mock2", catalogs[1].name());
    Assertions.assertEquals("comment2", catalogs[1].comment());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalogs[1].type());

    // Test return no found
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, params, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.listCatalogsInfo());
    Assertions.assertTrue(ex.getMessage().contains("mock error"));
  }

  @Test
  public void testLoadCatalog() throws JsonProcessingException {
    String catalogName = "mock";
    String path = "/api/metalakes/" + metalakeName + "/catalogs/" + catalogName;

    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName("mock")
            .withComment("comment")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogResponse resp = new CatalogResponse(mockCatalog);

    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);
    Catalog catalog = gravitinoClient.loadCatalog(catalogName);

    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals("comment", catalog.comment());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());

    // Test return not found
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchCatalogException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchCatalogException.class, () -> gravitinoClient.loadCatalog(catalogName));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test return unsupported catalog type
    CatalogDTO mockCatalog1 =
        CatalogDTO.builder()
            .withName("mock")
            .withComment("comment")
            .withType(Catalog.Type.UNSUPPORTED)
            .withProvider("test")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogResponse resp1 = new CatalogResponse(mockCatalog1);
    buildMockResource(Method.GET, path, null, resp1, HttpStatus.SC_OK);
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> gravitinoClient.loadCatalog(catalogName));

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 =
        Assertions.assertThrows(
            RuntimeException.class, () -> gravitinoClient.loadCatalog(catalogName));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test return unparsed system error
    buildMockResource(Method.GET, path, null, "mock error", HttpStatus.SC_CONFLICT);
    Throwable ex2 =
        Assertions.assertThrows(
            RESTException.class, () -> gravitinoClient.loadCatalog(catalogName));
    Assertions.assertTrue(ex2.getMessage().contains("Error code: " + HttpStatus.SC_CONFLICT));
  }

  @Test
  public void testCreateCatalog() throws JsonProcessingException {
    String catalogName = "mock";
    String path = "/api/metalakes/" + metalakeName + "/catalogs";

    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName(catalogName)
            .withComment("comment")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    CatalogCreateRequest req =
        new CatalogCreateRequest(
            catalogName, Catalog.Type.RELATIONAL, provider, "comment", Collections.emptyMap());
    CatalogResponse resp = new CatalogResponse(mockCatalog);
    buildMockResource(Method.POST, path, req, resp, HttpStatus.SC_OK);

    Catalog catalog =
        gravitinoClient.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, "comment", Collections.emptyMap());
    Assertions.assertEquals(catalogName, catalog.name());
    Assertions.assertEquals("comment", catalog.comment());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());

    // Test return unsupported catalog type
    CatalogDTO mockCatalog1 =
        CatalogDTO.builder()
            .withName("mock")
            .withComment("comment")
            .withType(Catalog.Type.UNSUPPORTED)
            .withProvider("test")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
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
        () ->
            gravitinoClient.createCatalog(
                id.name(), Catalog.Type.MESSAGING, provider, "comment", emptyMap));

    // Test return NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, path, req, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                gravitinoClient.createCatalog(
                    id.name(), Catalog.Type.RELATIONAL, provider, "comment", emptyMap));
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
                gravitinoClient.createCatalog(
                    id.name(), Catalog.Type.RELATIONAL, provider, "comment", emptyMap));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.POST, path, req, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex2 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                gravitinoClient.createCatalog(
                    id.name(), Catalog.Type.RELATIONAL, provider, "comment", emptyMap));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));
  }

  @Test
  public void testAlterCatalog() throws JsonProcessingException {
    String catalogName = "mock";
    String path = "/api/metalakes/" + metalakeName + "/catalogs/" + catalogName;

    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName("mock1")
            .withComment("comment1")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
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
    Catalog catalog = gravitinoClient.alterCatalog(catalogName, change1, change2);
    Assertions.assertEquals("mock1", catalog.name());
    Assertions.assertEquals("comment1", catalog.comment());
    Assertions.assertEquals(Catalog.Type.RELATIONAL, catalog.type());

    // Test return NoSuchCatalogException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchCatalogException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, path, updatesRequest, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchCatalogException.class,
            () -> gravitinoClient.alterCatalog(catalogName, change1, change2));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test return IllegalArgumentException
    ErrorResponse errorResponse1 = ErrorResponse.illegalArguments("mock error");
    buildMockResource(Method.PUT, path, updatesRequest, errorResponse1, HttpStatus.SC_BAD_REQUEST);
    Throwable ex1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> gravitinoClient.alterCatalog(catalogName, change1, change2));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test return internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(
        Method.PUT, path, updatesRequest, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex2 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> gravitinoClient.alterCatalog(catalogName, change1, change2));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));
  }

  @Test
  public void testDropCatalog() throws JsonProcessingException {
    String catalogName = "mock";
    String path = "/api/metalakes/" + metalakeName + "/catalogs/" + catalogName;

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, path, null, resp, HttpStatus.SC_OK);
    boolean dropped = gravitinoClient.dropCatalog(catalogName, true);
    Assertions.assertTrue(dropped, "catalog should be dropped");

    // Test return false
    DropResponse resp1 = new DropResponse(false);
    buildMockResource(Method.DELETE, path, null, resp1, HttpStatus.SC_OK);
    boolean dropped1 = gravitinoClient.dropCatalog(catalogName);
    Assertions.assertFalse(dropped1, "catalog should be non-existent");
  }

  @Test
  public void testListTags() throws JsonProcessingException {
    String path = "/api/metalakes/" + metalakeName + "/tags";

    String[] tagNames = new String[] {"tag1", "tag2"};
    NameListResponse resp = new NameListResponse(tagNames);
    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);

    String[] tags = gravitinoClient.listTags();
    Assertions.assertEquals(2, tags.length);
    Assertions.assertArrayEquals(tagNames, tags);

    // Test return empty tag list
    NameListResponse resp1 = new NameListResponse(new String[] {});
    buildMockResource(Method.GET, path, null, resp1, HttpStatus.SC_OK);
    String[] tags1 = gravitinoClient.listTags();
    Assertions.assertEquals(0, tags1.length);

    // Test throw MetalakeNotFoundException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(NoSuchMetalakeException.class, gravitinoClient::listTags);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 = Assertions.assertThrows(RuntimeException.class, gravitinoClient::listTags);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  @Test
  public void testListTagInfos() throws JsonProcessingException {
    String path = "/api/metalakes/" + metalakeName + "/tags";
    Map<String, String> params = Collections.singletonMap("details", "true");

    TagDTO tag1 =
        TagDTO.builder()
            .withName("tag1")
            .withComment("comment1")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    TagDTO tag2 =
        TagDTO.builder()
            .withName("tag2")
            .withComment("comment2")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    TagDTO[] tags = new TagDTO[] {tag1, tag2};
    TagListResponse resp = new TagListResponse(tags);
    buildMockResource(Method.GET, path, params, null, resp, HttpStatus.SC_OK);

    Tag[] tagInfos = gravitinoClient.listTagsInfo();
    Assertions.assertEquals(2, tagInfos.length);
    Assertions.assertEquals("tag1", tagInfos[0].name());
    Assertions.assertEquals("comment1", tagInfos[0].comment());
    Assertions.assertEquals("tag2", tagInfos[1].name());
    Assertions.assertEquals("comment2", tagInfos[1].comment());

    // Test empty tag list
    TagListResponse resp1 = new TagListResponse(new TagDTO[] {});
    buildMockResource(Method.GET, path, params, null, resp1, HttpStatus.SC_OK);
    Tag[] tagInfos1 = gravitinoClient.listTagsInfo();
    Assertions.assertEquals(0, tagInfos1.length);

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, params, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(NoSuchMetalakeException.class, gravitinoClient::listTagsInfo);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(
        Method.GET, path, params, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 = Assertions.assertThrows(RuntimeException.class, gravitinoClient::listTagsInfo);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  @Test
  public void testGetTag() throws JsonProcessingException {
    String tagName = "tag1";
    String path = "/api/metalakes/" + metalakeName + "/tags/" + tagName;
    TagDTO tag1 =
        TagDTO.builder()
            .withName(tagName)
            .withComment("comment1")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    TagResponse resp = new TagResponse(tag1);
    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);

    Tag tag = gravitinoClient.getTag(tagName);
    Assertions.assertEquals(tagName, tag.name());
    Assertions.assertEquals("comment1", tag.comment());

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.getTag(tagName));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw NoSuchTagException
    ErrorResponse errorResponse1 =
        ErrorResponse.notFound(NoSuchTagException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse1, HttpStatus.SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(NoSuchTagException.class, () -> gravitinoClient.getTag(tagName));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex2 =
        Assertions.assertThrows(RuntimeException.class, () -> gravitinoClient.getTag(tagName));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));
  }

  @Test
  public void testCreateTag() throws JsonProcessingException {
    String tagName = "tag1";
    String path = "/api/metalakes/" + metalakeName + "/tags";

    TagDTO tag1 =
        TagDTO.builder()
            .withName(tagName)
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    TagResponse resp = new TagResponse(tag1);

    TagCreateRequest req = new TagCreateRequest(tagName, null, null);
    buildMockResource(Method.POST, path, req, resp, HttpStatus.SC_OK);

    Tag tag = gravitinoClient.createTag(tagName, null, null);
    Assertions.assertEquals(tagName, tag.name());
    Assertions.assertNull(tag.comment());
    Assertions.assertNull(tag.properties());

    // Test with null name
    Throwable ex =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> gravitinoClient.createTag(null, null, null));
    Assertions.assertTrue(ex.getMessage().contains("tag name must not be null or empty"));

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.createTag(tagName, null, null));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw TagAlreadyExistsException
    ErrorResponse errorResponse1 =
        ErrorResponse.alreadyExists(TagAlreadyExistsException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, path, null, errorResponse1, HttpStatus.SC_CONFLICT);
    Throwable ex2 =
        Assertions.assertThrows(
            TagAlreadyExistsException.class, () -> gravitinoClient.createTag(tagName, null, null));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.POST, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex3 =
        Assertions.assertThrows(
            RuntimeException.class, () -> gravitinoClient.createTag(tagName, null, null));
    Assertions.assertTrue(ex3.getMessage().contains("mock error"));
  }

  @Test
  public void testAlterTag() throws JsonProcessingException {
    String tagName = "tag1";
    String path = "/api/metalakes/" + metalakeName + "/tags/" + tagName;

    TagDTO tag2 =
        TagDTO.builder()
            .withName("tag2")
            .withComment("comment2")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    TagResponse resp = new TagResponse(tag2);

    List<TagUpdateRequest> reqs =
        Arrays.asList(
            new TagUpdateRequest.RenameTagRequest("tag2"),
            new TagUpdateRequest.UpdateTagCommentRequest("comment2"));
    TagUpdatesRequest request = new TagUpdatesRequest(reqs);
    buildMockResource(Method.PUT, path, request, resp, HttpStatus.SC_OK);

    Tag tag =
        gravitinoClient.alterTag(
            tagName, TagChange.rename("tag2"), TagChange.updateComment("comment2"));
    Assertions.assertEquals("tag2", tag.name());
    Assertions.assertEquals("comment2", tag.comment());
    Assertions.assertNull(tag.properties());

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, path, request, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                gravitinoClient.alterTag(
                    tagName, TagChange.rename("tag2"), TagChange.updateComment("comment2")));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw NoSuchTagException
    ErrorResponse errorResponse1 =
        ErrorResponse.notFound(NoSuchTagException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, path, request, errorResponse1, HttpStatus.SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(
            NoSuchTagException.class,
            () ->
                gravitinoClient.alterTag(
                    tagName, TagChange.rename("tag2"), TagChange.updateComment("comment2")));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw IllegalArgumentException
    ErrorResponse errorResponse2 = ErrorResponse.illegalArguments("mock error");
    buildMockResource(Method.PUT, path, request, errorResponse2, HttpStatus.SC_BAD_REQUEST);
    Throwable ex2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                gravitinoClient.alterTag(
                    tagName, TagChange.rename("tag2"), TagChange.updateComment("comment2")));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.PUT, path, request, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                gravitinoClient.alterTag(
                    tagName, TagChange.rename("tag2"), TagChange.updateComment("comment2")));
    Assertions.assertTrue(ex3.getMessage().contains("mock error"));
  }

  @Test
  public void testDeleteTag() throws JsonProcessingException {
    String tagName = "tag1";
    String path = "/api/metalakes/" + metalakeName + "/tags/" + tagName;

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, path, null, resp, HttpStatus.SC_OK);
    boolean dropped = gravitinoClient.deleteTag(tagName);
    Assertions.assertTrue(dropped);

    // Test return false
    DropResponse resp1 = new DropResponse(false);
    buildMockResource(Method.DELETE, path, null, resp1, HttpStatus.SC_OK);
    boolean dropped1 = gravitinoClient.deleteTag(tagName);
    Assertions.assertFalse(dropped1);

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.DELETE, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.deleteTag(tagName));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.DELETE, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, () -> gravitinoClient.deleteTag(tagName));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  @Test
  public void testListPolicies() throws JsonProcessingException {
    String path = "/api/metalakes/" + metalakeName + "/policies";

    String[] policyNames = new String[] {"policy1", "policy2"};
    NameListResponse resp = new NameListResponse(policyNames);
    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);

    String[] policies = gravitinoClient.listPolicies();
    Assertions.assertEquals(2, policies.length);
    Assertions.assertArrayEquals(policyNames, policies);

    // Test return empty policy list
    NameListResponse resp1 = new NameListResponse(new String[] {});
    buildMockResource(Method.GET, path, null, resp1, HttpStatus.SC_OK);
    String[] policies1 = gravitinoClient.listPolicies();
    Assertions.assertEquals(0, policies1.length);

    // Test throw MetalakeNotFoundException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(NoSuchMetalakeException.class, gravitinoClient::listPolicies);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 = Assertions.assertThrows(RuntimeException.class, gravitinoClient::listPolicies);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  @Test
  public void testListPolicyInfos() throws JsonProcessingException {
    String path = "/api/metalakes/" + metalakeName + "/policies";
    Map<String, String> params = Collections.singletonMap("details", "true");
    Set<MetadataObject.Type> supportedTypes = Collections.singleton(MetadataObject.Type.TABLE);

    PolicyDTO policy1 =
        PolicyDTO.builder()
            .withName("policy1")
            .withComment("comment1")
            .withPolicyType("custom")
            .withContent(
                PolicyContentDTO.CustomContentDTO.builder()
                    .withSupportedObjectTypes(supportedTypes)
                    .build())
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    PolicyDTO policy2 =
        PolicyDTO.builder()
            .withName("policy2")
            .withComment("comment2")
            .withPolicyType("custom")
            .withContent(
                PolicyContentDTO.CustomContentDTO.builder()
                    .withSupportedObjectTypes(supportedTypes)
                    .build())
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    PolicyDTO[] policies = new PolicyDTO[] {policy1, policy2};
    PolicyListResponse resp = new PolicyListResponse(policies);
    buildMockResource(Method.GET, path, params, null, resp, HttpStatus.SC_OK);

    Policy[] policyInfos = gravitinoClient.listPolicyInfos();
    Assertions.assertEquals(2, policyInfos.length);
    Assertions.assertEquals(policy1.name(), policyInfos[0].name());
    Assertions.assertEquals(policy1.comment(), policyInfos[0].comment());
    Assertions.assertEquals(fromDTO(policy1.content()), policyInfos[0].content());
    Assertions.assertEquals(policy2.name(), policyInfos[1].name());
    Assertions.assertEquals(policy2.comment(), policyInfos[1].comment());
    Assertions.assertEquals(fromDTO(policy2.content()), policyInfos[1].content());

    // Test empty policy list
    PolicyListResponse resp1 = new PolicyListResponse(new PolicyDTO[] {});
    buildMockResource(Method.GET, path, params, null, resp1, HttpStatus.SC_OK);
    Policy[] policyInfos1 = gravitinoClient.listPolicyInfos();
    Assertions.assertEquals(0, policyInfos1.length);

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, params, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(NoSuchMetalakeException.class, gravitinoClient::listPolicyInfos);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(
        Method.GET, path, params, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, gravitinoClient::listPolicyInfos);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  @Test
  public void testGetPolicy() throws JsonProcessingException {
    String policyName = "policy1";
    String path = "/api/metalakes/" + metalakeName + "/policies/" + policyName;

    Set<MetadataObject.Type> supportedTypes = Collections.singleton(MetadataObject.Type.TABLE);
    PolicyDTO policy1 =
        PolicyDTO.builder()
            .withName(policyName)
            .withComment("comment1")
            .withPolicyType("custom")
            .withContent(
                PolicyContentDTO.CustomContentDTO.builder()
                    .withSupportedObjectTypes(supportedTypes)
                    .build())
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    PolicyResponse resp = new PolicyResponse(policy1);
    buildMockResource(Method.GET, path, null, resp, HttpStatus.SC_OK);

    Policy policy = gravitinoClient.getPolicy(policyName);
    Assertions.assertEquals(policyName, policy.name());
    Assertions.assertEquals((policy1.comment()), policy.comment());
    Assertions.assertEquals(fromDTO(policy1.content()), policy.content());

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.getPolicy(policyName));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw NoSuchPolicyException
    ErrorResponse errorResponse1 =
        ErrorResponse.notFound(NoSuchPolicyException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResponse1, HttpStatus.SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(
            NoSuchPolicyException.class, () -> gravitinoClient.getPolicy(policyName));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex2 =
        Assertions.assertThrows(
            RuntimeException.class, () -> gravitinoClient.getPolicy(policyName));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));
  }

  @Test
  public void testCreatePolicy() throws JsonProcessingException {
    String policyName = "policy1";
    String path = "/api/metalakes/" + metalakeName + "/policies";

    Set<MetadataObject.Type> supportedTypes = ImmutableSet.of(MetadataObject.Type.TABLE);
    PolicyDTO policy1 =
        PolicyDTO.builder()
            .withName(policyName)
            .withPolicyType("custom")
            .withContent(
                PolicyContentDTO.CustomContentDTO.builder()
                    .withSupportedObjectTypes(supportedTypes)
                    .build())
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    PolicyResponse resp = new PolicyResponse(policy1);

    PolicyCreateRequest req =
        new PolicyCreateRequest(
            policyName, "custom", policy1.comment(), policy1.enabled(), policy1.content());
    buildMockResource(Method.POST, path, req, resp, HttpStatus.SC_OK);

    Policy policy =
        gravitinoClient.createPolicy(
            policyName,
            "custom",
            policy1.comment(),
            policy1.enabled(),
            PolicyContents.custom(null, supportedTypes, null));
    Assertions.assertEquals(policyName, policy.name());
    Assertions.assertNull(policy.comment());

    // Test with null name
    Throwable ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                gravitinoClient.createPolicy(
                    null,
                    "custom",
                    policy1.comment(),
                    policy1.enabled(),
                    PolicyContents.custom(null, supportedTypes, null)));
    Assertions.assertEquals("\"name\" is required and cannot be empty", ex.getMessage());

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                gravitinoClient.createPolicy(
                    policyName,
                    "custom",
                    policy1.comment(),
                    policy1.enabled(),
                    PolicyContents.custom(null, supportedTypes, null)));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw PolicyAlreadyExistsException
    ErrorResponse errorResponse1 =
        ErrorResponse.alreadyExists(
            PolicyAlreadyExistsException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, path, null, errorResponse1, HttpStatus.SC_CONFLICT);
    Throwable ex2 =
        Assertions.assertThrows(
            PolicyAlreadyExistsException.class,
            () ->
                gravitinoClient.createPolicy(
                    policyName,
                    "custom",
                    policy1.comment(),
                    policy1.enabled(),
                    PolicyContents.custom(null, supportedTypes, null)));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.POST, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                gravitinoClient.createPolicy(
                    policyName,
                    "custom",
                    policy1.comment(),
                    policy1.enabled(),
                    PolicyContents.custom(null, supportedTypes, null)));
    Assertions.assertTrue(ex3.getMessage().contains("mock error"));
  }

  @Test
  public void testAlterPolicy() throws JsonProcessingException {
    String policyName = "policy1";
    String path = "/api/metalakes/" + metalakeName + "/policies/" + policyName;

    Set<MetadataObject.Type> supportedTypes = ImmutableSet.of(MetadataObject.Type.TABLE);
    PolicyDTO policy2 =
        PolicyDTO.builder()
            .withName("policy2")
            .withComment("comment2")
            .withPolicyType("custom")
            .withContent(
                PolicyContentDTO.CustomContentDTO.builder()
                    .withSupportedObjectTypes(supportedTypes)
                    .build())
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    PolicyResponse resp = new PolicyResponse(policy2);

    PolicyContentDTO newContent =
        PolicyContentDTO.CustomContentDTO.builder()
            .withCustomRules(ImmutableMap.of("rule1", "value1"))
            .withSupportedObjectTypes(supportedTypes)
            .build();
    List<PolicyUpdateRequest> reqs =
        Arrays.asList(
            new PolicyUpdateRequest.RenamePolicyRequest("policy2"),
            new PolicyUpdateRequest.UpdatePolicyCommentRequest("comment2"),
            new PolicyUpdateRequest.UpdatePolicyContentRequest("custom", newContent));
    PolicyUpdatesRequest request = new PolicyUpdatesRequest(reqs);
    buildMockResource(Method.PUT, path, request, resp, HttpStatus.SC_OK);

    Policy policy =
        gravitinoClient.alterPolicy(
            policyName,
            PolicyChange.rename("policy2"),
            PolicyChange.updateComment("comment2"),
            PolicyChange.updateContent(
                "custom",
                PolicyContents.custom(ImmutableMap.of("rule1", "value1"), supportedTypes, null)));
    Assertions.assertEquals("policy2", policy.name());
    Assertions.assertEquals("comment2", policy.comment());

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, path, request, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                gravitinoClient.alterPolicy(
                    policyName,
                    PolicyChange.rename("policy2"),
                    PolicyChange.updateComment("comment2"),
                    PolicyChange.updateContent(
                        "custom",
                        PolicyContents.custom(
                            ImmutableMap.of("rule1", "value1"), supportedTypes, null))));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw NoSuchPolicyException
    ErrorResponse errorResponse1 =
        ErrorResponse.notFound(NoSuchPolicyException.class.getSimpleName(), "mock error");
    buildMockResource(Method.PUT, path, request, errorResponse1, HttpStatus.SC_NOT_FOUND);
    Throwable ex1 =
        Assertions.assertThrows(
            NoSuchPolicyException.class,
            () ->
                gravitinoClient.alterPolicy(
                    policyName,
                    PolicyChange.rename("policy2"),
                    PolicyChange.updateComment("comment2"),
                    PolicyChange.updateContent(
                        "custom",
                        PolicyContents.custom(
                            ImmutableMap.of("rule1", "value1"), supportedTypes, null))));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));

    // Test throw IllegalArgumentException
    ErrorResponse errorResponse2 = ErrorResponse.illegalArguments("mock error");
    buildMockResource(Method.PUT, path, request, errorResponse2, HttpStatus.SC_BAD_REQUEST);
    Throwable ex2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                gravitinoClient.alterPolicy(
                    policyName,
                    PolicyChange.rename("policy2"),
                    PolicyChange.updateComment("comment2"),
                    PolicyChange.updateContent(
                        "custom",
                        PolicyContents.custom(
                            ImmutableMap.of("rule1", "value1"), supportedTypes, null))));
    Assertions.assertTrue(ex2.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.PUT, path, request, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                gravitinoClient.alterPolicy(
                    policyName,
                    PolicyChange.rename("policy2"),
                    PolicyChange.updateComment("comment2"),
                    PolicyChange.updateContent(
                        "custom",
                        PolicyContents.custom(
                            ImmutableMap.of("rule1", "value1"),
                            ImmutableSet.of(MetadataObject.Type.TABLE),
                            null))));
    Assertions.assertTrue(ex3.getMessage().contains("mock error"));
  }

  @Test
  public void testDeletePolicy() throws JsonProcessingException {
    String policyName = "policy1";
    String path = "/api/metalakes/" + metalakeName + "/policies/" + policyName;

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, path, null, resp, HttpStatus.SC_OK);
    boolean dropped = gravitinoClient.deletePolicy(policyName);
    Assertions.assertTrue(dropped);

    // Test return false
    DropResponse resp1 = new DropResponse(false);
    buildMockResource(Method.DELETE, path, null, resp1, HttpStatus.SC_OK);
    boolean dropped1 = gravitinoClient.deletePolicy(policyName);
    Assertions.assertFalse(dropped1);

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResponse =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.DELETE, path, null, errorResponse, HttpStatus.SC_NOT_FOUND);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.deletePolicy(policyName));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test internal error
    ErrorResponse errorResp = ErrorResponse.internalError("mock error");
    buildMockResource(Method.DELETE, path, null, errorResp, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 =
        Assertions.assertThrows(
            RuntimeException.class, () -> gravitinoClient.deletePolicy(policyName));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  @Test
  public void testEquals() throws JsonProcessingException {
    GravitinoMetalake metalake1 = createMetalake(client, "test", true);
    GravitinoMetalake metalake2 = createMetalake(client, "test", true);
    GravitinoMetalake metalake3 = createMetalake(client, "another-test", true);
    GravitinoMetalake metalake4 = createMetalake(client, "test");
    Assertions.assertEquals(metalake1, metalake1);
    Assertions.assertEquals(metalake1, metalake2);
    Assertions.assertNotEquals(metalake1, metalake3);
    Assertions.assertNotEquals(metalake1, metalake4);
    Assertions.assertNotEquals(metalake1, null);
    Assertions.assertNotEquals(metalake1, new Object());
  }

  static GravitinoMetalake createMetalake(GravitinoAdminClient client, String metalakeName)
      throws JsonProcessingException {
    return createMetalake(client, metalakeName, false);
  }

  static GravitinoMetalake createMetalake(
      GravitinoAdminClient client, String metalakeName, boolean fixedAudit)
      throws JsonProcessingException {
    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName(metalakeName)
            .withComment("comment")
            .withAudit(
                AuditDTO.builder()
                    .withCreator("creator")
                    .withCreateTime(fixedAudit ? testStartTime : Instant.now())
                    .build())
            .build();
    MetalakeCreateRequest req =
        new MetalakeCreateRequest(metalakeName, "comment", Collections.emptyMap());
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.POST, "/api/metalakes", req, resp, HttpStatus.SC_OK);

    return client.createMetalake(metalakeName, "comment", Collections.emptyMap());
  }
}
