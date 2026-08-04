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

import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.messaging.DataLayoutDTO;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.TopicCreateRequest;
import org.apache.gravitino.dto.requests.TopicUpdateRequest;
import org.apache.gravitino.dto.requests.TopicUpdatesRequest;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.TopicResponse;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.DataLayouts;
import org.apache.gravitino.messaging.SchemaDataLayout;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicChange;
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
            catalogName,
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
    NameIdentifier[] topics = ((MessagingCatalog) catalog).listTopics(Namespace.of("schema1"));

    NameIdentifier expectedResultTopic1 = NameIdentifier.of("schema1", "topic1");
    NameIdentifier expectedResultTopic2 = NameIdentifier.of("schema1", "topic2");
    Assertions.assertEquals(2, topics.length);
    Assertions.assertEquals(expectedResultTopic1, topics[0]);
    Assertions.assertEquals(expectedResultTopic2, topics[1]);

    // Throw schema not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, topicPath, null, errResp, SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asTopicCatalog().listTopics(expectedResultTopic1.namespace()),
        "schema not found");

    // Throw Runtime exception
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, topicPath, null, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asTopicCatalog().listTopics(expectedResultTopic1.namespace()),
        "internal error");
  }

  @Test
  public void testLoadTopic() throws JsonProcessingException {
    NameIdentifier topic = NameIdentifier.of("schema1", "topic1");
    String topicPath =
        withSlash(
            MessagingCatalog.formatTopicRequestPath(
                    Namespace.of(metalakeName, catalogName, "schema1"))
                + "/"
                + topic.name());

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
    NameIdentifier topic = NameIdentifier.of("schema1", "topic1");
    String topicPath =
        withSlash(
            MessagingCatalog.formatTopicRequestPath(
                Namespace.of(metalakeName, catalogName, "schema1")));

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
  public void testCreateTopicWithDataLayouts() throws JsonProcessingException {
    NameIdentifier topic = NameIdentifier.of("schema1", "topic_with_layouts");
    String topicPath =
        withSlash(
            MessagingCatalog.formatTopicRequestPath(
                Namespace.of(metalakeName, catalogName, "schema1")));
    DataLayout keyLayout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.JSON)
            .withSchemaSubject("order-key")
            .build();
    DataLayout valueLayout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.PROTOBUF)
            .withSchemaSubject("order-value")
            .build();
    ImmutableMap<String, DataLayout> dataLayouts =
        ImmutableMap.copyOf(DataLayouts.of(keyLayout, valueLayout));

    TopicDTO mockTopic =
        mockTopicDTO(
            topic.name(), "comment", ImmutableMap.of("k1", "k2"), toDataLayoutDTOs(dataLayouts));
    TopicCreateRequest topicCreateRequest =
        new TopicCreateRequest(
            topic.name(), "comment", ImmutableMap.of("k1", "k2"), toDataLayoutDTOs(dataLayouts));
    TopicResponse topicResponse = new TopicResponse(mockTopic);
    buildMockResource(Method.POST, topicPath, topicCreateRequest, topicResponse, SC_OK);

    Topic createdTopic =
        catalog
            .asTopicCatalog()
            .createTopic(topic, "comment", dataLayouts, ImmutableMap.of("k1", "k2"));

    Assertions.assertNotNull(createdTopic);
    assertTopic(mockTopic, createdTopic);
  }

  @Test
  public void testAlterTopic() throws JsonProcessingException {
    NameIdentifier topic = NameIdentifier.of("schema1", "topic1");
    String topicPath =
        withSlash(
            MessagingCatalog.formatTopicRequestPath(
                    Namespace.of(metalakeName, catalogName, "schema1"))
                + "/"
                + topic.name());

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

    // test update topic data layout
    DataLayout valueLayout =
        SchemaDataLayout.builder()
            .withFormat(DataLayout.Format.AVRO)
            .withSchemaSubject("order-value")
            .build();
    TopicUpdateRequest req4 =
        new TopicUpdateRequest.UpdateTopicDataLayoutRequest(
            DataLayouts.VALUE, DataLayoutDTO.fromDataLayout(valueLayout));
    TopicDTO mockTopic4 =
        mockTopicDTO(
            topic.name(),
            "new comment",
            ImmutableMap.of("k1", "v1"),
            toDataLayoutDTOs(DataLayouts.ofValue(valueLayout)));
    TopicResponse resp4 = new TopicResponse(mockTopic4);
    buildMockResource(
        Method.PUT, topicPath, new TopicUpdatesRequest(ImmutableList.of(req4)), resp4, SC_OK);

    alteredTopic =
        catalog
            .asTopicCatalog()
            .alterTopic(topic, TopicChange.updateDataLayout(DataLayouts.VALUE, valueLayout));
    assertTopic(mockTopic4, alteredTopic);

    // test remove topic data layout
    TopicUpdateRequest req5 = new TopicUpdateRequest.RemoveTopicDataLayoutRequest(DataLayouts.KEY);
    TopicDTO mockTopic5 =
        mockTopicDTO(
            topic.name(),
            "new comment",
            ImmutableMap.of("k1", "v1"),
            toDataLayoutDTOs(DataLayouts.ofValue(valueLayout)));
    TopicResponse resp5 = new TopicResponse(mockTopic5);
    buildMockResource(
        Method.PUT, topicPath, new TopicUpdatesRequest(ImmutableList.of(req5)), resp5, SC_OK);

    alteredTopic =
        catalog.asTopicCatalog().alterTopic(topic, TopicChange.removeDataLayout(DataLayouts.KEY));
    assertTopic(mockTopic5, alteredTopic);

    // test remove all topic data layouts
    TopicUpdateRequest req6 = new TopicUpdateRequest.RemoveTopicDataLayoutsRequest();
    TopicDTO mockTopic6 = mockTopicDTO(topic.name(), "new comment", ImmutableMap.of("k1", "v1"));
    TopicResponse resp6 = new TopicResponse(mockTopic6);
    buildMockResource(
        Method.PUT, topicPath, new TopicUpdatesRequest(ImmutableList.of(req6)), resp6, SC_OK);

    alteredTopic = catalog.asTopicCatalog().alterTopic(topic, TopicChange.removeDataLayouts());
    assertTopic(mockTopic6, alteredTopic);

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
    NameIdentifier topic = NameIdentifier.of("schema1", "topic1");
    String topicPath =
        withSlash(
            MessagingCatalog.formatTopicRequestPath(
                    Namespace.of(metalakeName, catalogName, "schema1"))
                + "/"
                + topic.name());
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
    return mockTopicDTO(name, comment, properties, null);
  }

  private TopicDTO mockTopicDTO(
      String name,
      String comment,
      ImmutableMap<String, String> properties,
      ImmutableMap<String, DataLayoutDTO> dataLayouts) {
    return TopicDTO.builder()
        .withName(name)
        .withComment(comment)
        .withProperties(properties)
        .withDataLayouts(dataLayouts)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private void assertTopic(TopicDTO expected, Topic actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.properties(), actual.properties());
    Assertions.assertEquals(expected.dataLayouts(), actual.dataLayouts());
  }

  private ImmutableMap<String, DataLayoutDTO> toDataLayoutDTOs(
      Map<String, ? extends DataLayout> dataLayouts) {
    ImmutableMap.Builder<String, DataLayoutDTO> builder = ImmutableMap.builder();
    dataLayouts.forEach((name, layout) -> builder.put(name, DataLayoutDTO.fromDataLayout(layout)));
    return builder.build();
  }
}
