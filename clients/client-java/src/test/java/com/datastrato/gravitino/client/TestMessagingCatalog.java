/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.messaging.TopicDTO;
import com.datastrato.gravitino.dto.requests.CatalogCreateRequest;
import com.datastrato.gravitino.dto.requests.TopicCreateRequest;
import com.datastrato.gravitino.dto.requests.TopicUpdateRequest;
import com.datastrato.gravitino.dto.requests.TopicUpdatesRequest;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.TopicResponse;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.messaging.Topic;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestMessagingCatalog extends TestBase {
  protected static Catalog catalog;

  private static GravitinoMetalake metalake;

  protected static final String metalakeName = "testMetalake";

  protected static final String catalogName = "testMessagingCatalog";

  private static final String provider = "test";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    metalake = TestGravitinoMetalake.createMetalake(client, metalakeName);
    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName(catalogName)
            .withType(CatalogDTO.Type.MESSAGING)
            .withProvider(provider)
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "k2"))
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    CatalogCreateRequest catalogCreateRequest =
        new CatalogCreateRequest(
            catalogName,
            CatalogDTO.Type.MESSAGING,
            provider,
            "comment",
            ImmutableMap.of("k1", "k2"));
    CatalogResponse catalogResponse = new CatalogResponse(mockCatalog);
    buildMockResource(
        Method.POST,
        "/api/metalakes/" + metalakeName + "/catalogs",
        catalogCreateRequest,
        catalogResponse,
        SC_OK);

    catalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            CatalogDTO.Type.MESSAGING,
            provider,
            "comment",
            ImmutableMap.of("k1", "k2"));
  }

  @Test
  public void testListTopics() throws Exception {
    NameIdentifier topic1 = NameIdentifier.of(metalakeName, catalogName, "schema1", "topic1");
    NameIdentifier topic2 = NameIdentifier.of(metalakeName, catalogName, "schema1", "topic2");
    String topicPath = withSlash(MessagingCatalog.formatTopicRequestPath(topic1.namespace()));

    EntityListResponse entityListResponse =
        new EntityListResponse(new NameIdentifier[] {topic1, topic2});
    buildMockResource(Method.GET, topicPath, null, entityListResponse, SC_OK);
    NameIdentifier[] topics = ((MessagingCatalog) catalog).listTopics(topic1.namespace());

    Assertions.assertEquals(2, topics.length);
    Assertions.assertEquals(topic1, topics[0]);
    Assertions.assertEquals(topic2, topics[1]);

    // Throw schema not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, topicPath, null, errResp, SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asTopicCatalog().listTopics(topic1.namespace()),
        "schema not found");

    // Throw Runtime exception
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, topicPath, null, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asTopicCatalog().listTopics(topic1.namespace()),
        "internal error");
  }

  @Test
  public void testLoadTopic() throws JsonProcessingException {
    NameIdentifier topic = NameIdentifier.of(metalakeName, catalogName, "schema1", "topic1");
    String topicPath =
        withSlash(MessagingCatalog.formatTopicRequestPath(topic.namespace()) + "/" + topic.name());

    TopicDTO mockTopic = mockTopicDTO(topic.name(), "comment", ImmutableMap.of("k1", "k2"));
    TopicResponse topicResponse = new TopicResponse(mockTopic);
    buildMockResource(Method.GET, topicPath, null, topicResponse, SC_OK);

    Topic loadedTopic = catalog.asTopicCatalog().loadTopic(topic);
    Assertions.assertNotNull(loadedTopic);
    assertTopic(mockTopic, loadedTopic);

    // test NoSuchSchemaException
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, topicPath, null, errResp, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(
            NoSuchSchemaException.class, () -> catalog.asTopicCatalog().loadTopic(topic));
    Assertions.assertEquals("schema not found", ex.getMessage());
  }

  @Test
  public void testCreateTopic() throws JsonProcessingException {
    NameIdentifier topic = NameIdentifier.of(metalakeName, catalogName, "schema1", "topic1");
    String topicPath = withSlash(MessagingCatalog.formatTopicRequestPath(topic.namespace()));

    TopicDTO mockTopic = mockTopicDTO(topic.name(), "comment", ImmutableMap.of("k1", "k2"));

    TopicCreateRequest topicCreateRequest =
        new TopicCreateRequest(topic.name(), "comment", ImmutableMap.of("k1", "k2"));
    TopicResponse topicResponse = new TopicResponse(mockTopic);
    buildMockResource(Method.POST, topicPath, topicCreateRequest, topicResponse, SC_OK);

    Topic createdTopic =
        catalog.asTopicCatalog().createTopic(topic, "comment", null, ImmutableMap.of("k1", "k2"));
    Assertions.assertNotNull(createdTopic);
    assertTopic(mockTopic, createdTopic);

    // test NoSuchSchemaException
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.POST, topicPath, topicCreateRequest, errResp, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(
            NoSuchSchemaException.class,
            () ->
                catalog
                    .asTopicCatalog()
                    .createTopic(topic, "comment", null, ImmutableMap.of("k1", "k2")));
    Assertions.assertEquals("schema not found", ex.getMessage());

    // test TopicAlreadyExistsException
    ErrorResponse errResp2 =
        ErrorResponse.alreadyExists(
            TopicAlreadyExistsException.class.getSimpleName(), "topic already exists");
    buildMockResource(Method.POST, topicPath, topicCreateRequest, errResp2, SC_CONFLICT);

    Exception ex2 =
        Assertions.assertThrows(
            TopicAlreadyExistsException.class,
            () ->
                catalog
                    .asTopicCatalog()
                    .createTopic(topic, "comment", null, ImmutableMap.of("k1", "k2")));
    Assertions.assertEquals("topic already exists", ex2.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, topicPath, topicCreateRequest, errResp3, SC_SERVER_ERROR);
    Exception ex3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                catalog
                    .asTopicCatalog()
                    .createTopic(topic, "comment", null, ImmutableMap.of("k1", "k2")));
    Assertions.assertEquals("internal error", ex3.getMessage());
  }

  @Test
  public void testAlterTopic() throws JsonProcessingException {
    NameIdentifier topic = NameIdentifier.of(metalakeName, catalogName, "schema1", "topic1");
    String topicPath =
        withSlash(MessagingCatalog.formatTopicRequestPath(topic.namespace()) + "/" + topic.name());

    // test alter topic comment
    TopicUpdateRequest req1 = new TopicUpdateRequest.UpdateTopicCommentRequest("new comment");
    TopicDTO mockTopic1 = mockTopicDTO(topic.name(), "new comment", ImmutableMap.of("k1", "v1"));
    TopicResponse resp1 = new TopicResponse(mockTopic1);
    buildMockResource(
        Method.PUT, topicPath, new TopicUpdatesRequest(ImmutableList.of(req1)), resp1, SC_OK);

    Topic alteredTopic = catalog.asTopicCatalog().alterTopic(topic, req1.topicChange());
    assertTopic(mockTopic1, alteredTopic);

    // test set topic properties
    TopicUpdateRequest req2 = new TopicUpdateRequest.SetTopicPropertyRequest("k2", "v2");
    TopicDTO mockTopic2 =
        mockTopicDTO(topic.name(), "new comment", ImmutableMap.of("k1", "v1", "k2", "v2"));
    TopicResponse resp2 = new TopicResponse(mockTopic2);
    buildMockResource(
        Method.PUT, topicPath, new TopicUpdatesRequest(ImmutableList.of(req2)), resp2, SC_OK);

    alteredTopic = catalog.asTopicCatalog().alterTopic(topic, req2.topicChange());
    assertTopic(mockTopic2, alteredTopic);

    // test remove topic properties
    TopicUpdateRequest req3 = new TopicUpdateRequest.RemoveTopicPropertyRequest("k2");
    TopicDTO mockTopic3 = mockTopicDTO(topic.name(), "new comment", ImmutableMap.of("k1", "v1"));
    TopicResponse resp3 = new TopicResponse(mockTopic3);
    buildMockResource(
        Method.PUT, topicPath, new TopicUpdatesRequest(ImmutableList.of(req3)), resp3, SC_OK);

    alteredTopic = catalog.asTopicCatalog().alterTopic(topic, req3.topicChange());
    assertTopic(mockTopic3, alteredTopic);

    // test NoSuchTopicException
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchTopicException.class.getSimpleName(), "topic not found");
    buildMockResource(
        Method.PUT,
        topicPath,
        new TopicUpdatesRequest(ImmutableList.of(req1)),
        errResp,
        SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(
            NoSuchTopicException.class,
            () -> catalog.asTopicCatalog().alterTopic(topic, req1.topicChange()));
    Assertions.assertEquals("topic not found", ex.getMessage());

    // test RuntimeException
    errResp = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.PUT,
        topicPath,
        new TopicUpdatesRequest(ImmutableList.of(req1)),
        errResp,
        SC_SERVER_ERROR);
    ex =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> catalog.asTopicCatalog().alterTopic(topic, req1.topicChange()));
    Assertions.assertEquals("internal error", ex.getMessage());
  }

  @Test
  public void testDropTopic() throws JsonProcessingException {
    NameIdentifier topic = NameIdentifier.of(metalakeName, catalogName, "schema1", "topic1");
    String topicPath =
        withSlash(MessagingCatalog.formatTopicRequestPath(topic.namespace()) + "/" + topic.name());
    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, topicPath, null, resp, SC_OK);

    Assertions.assertTrue(catalog.asTopicCatalog().dropTopic(topic));

    resp = new DropResponse(false);
    buildMockResource(Method.DELETE, topicPath, null, resp, SC_OK);
    Assertions.assertFalse(catalog.asTopicCatalog().dropTopic(topic));

    // test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, topicPath, null, errResp, SC_SERVER_ERROR);
    Exception ex =
        Assertions.assertThrows(
            RuntimeException.class, () -> catalog.asTopicCatalog().dropTopic(topic));
    Assertions.assertEquals("internal error", ex.getMessage());
  }

  private TopicDTO mockTopicDTO(
      String name, String comment, ImmutableMap<String, String> properties) {
    return TopicDTO.builder()
        .withName(name)
        .withComment(comment)
        .withProperties(properties)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private void assertTopic(TopicDTO expected, Topic actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.properties(), actual.properties());
  }
}
