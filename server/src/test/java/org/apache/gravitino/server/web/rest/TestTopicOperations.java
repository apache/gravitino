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
package org.apache.gravitino.server.web.rest;

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.catalog.TopicOperationDispatcher;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.dto.requests.TopicCreateRequest;
import org.apache.gravitino.dto.requests.TopicUpdateRequest;
import org.apache.gravitino.dto.requests.TopicUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.TopicResponse;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestTopicOperations extends BaseOperationsTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private TopicOperationDispatcher dispatcher = mock(TopicOperationDispatcher.class);
  private final String metalake = "metalake";
  private final String catalog = "catalog1";
  private final String schema = "default";

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(TopicOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(TopicDispatcher.class).ranked(2);
            bindFactory(TestTopicOperations.MockServletRequestFactory.class)
                .to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListTopics() {
    NameIdentifier topic1 = NameIdentifier.of(metalake, catalog, schema, "topic1");
    NameIdentifier topic2 = NameIdentifier.of(metalake, catalog, schema, "topic2");

    when(dispatcher.listTopics(any())).thenReturn(new NameIdentifier[] {topic1, topic2});

    Response resp =
        target(topicPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    EntityListResponse listResp = resp.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    NameIdentifier[] topics = listResp.identifiers();
    Assertions.assertEquals(2, topics.length);
    Assertions.assertEquals(topic1, topics[0]);
    Assertions.assertEquals(topic2, topics[1]);

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error")).when(dispatcher).listTopics(any());
    Response resp1 =
        target(topicPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).listTopics(any());
    Response resp2 =
        target(topicPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testLoadTopic() {
    Topic topic = mockTopic("topic1", "comment", ImmutableMap.of("key1", "value1"));
    when(dispatcher.loadTopic(any())).thenReturn(topic);

    Response resp =
        target(topicPath(metalake, catalog, schema) + "/topic1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    TopicResponse topicResp = resp.readEntity(TopicResponse.class);
    Assertions.assertEquals(0, topicResp.getCode());

    TopicDTO topicDTO = topicResp.getTopic();
    Assertions.assertEquals("topic1", topicDTO.name());
    Assertions.assertEquals("comment", topicDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("key1", "value1"), topicDTO.properties());

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error")).when(dispatcher).loadTopic(any());
    Response resp1 =
        target(topicPath(metalake, catalog, schema) + "/topic1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).loadTopic(any());

    Response resp2 =
        target(topicPath(metalake, catalog, schema) + "/topic1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testCreateTopic() {
    Topic topic = mockTopic("topic1", "comment", ImmutableMap.of("key1", "value1"));
    when(dispatcher.createTopic(any(), any(), any(), any())).thenReturn(topic);

    TopicCreateRequest req =
        TopicCreateRequest.builder()
            .name("topic1")
            .comment("comment")
            .properties(ImmutableMap.of("key1", "value1"))
            .build();
    Response resp =
        target(topicPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    TopicResponse topicResp = resp.readEntity(TopicResponse.class);
    Assertions.assertEquals(0, topicResp.getCode());

    TopicDTO topicDTO = topicResp.getTopic();
    Assertions.assertEquals("topic1", topicDTO.name());
    Assertions.assertEquals("comment", topicDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("key1", "value1"), topicDTO.properties());

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error"))
        .when(dispatcher)
        .createTopic(any(), any(), any(), any());

    Response resp1 =
        target(topicPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw TopicAlreadyExistsException
    doThrow(new TopicAlreadyExistsException("mock error"))
        .when(dispatcher)
        .createTopic(any(), any(), any(), any());

    Response resp2 =
        target(topicPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp2.getCode());
    Assertions.assertEquals(
        TopicAlreadyExistsException.class.getSimpleName(), errorResp2.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(dispatcher)
        .createTopic(any(), any(), any(), any());

    Response resp3 =
        target(topicPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testSetTopicProperties() {
    TopicUpdateRequest req = new TopicUpdateRequest.SetTopicPropertyRequest("key1", "value1");
    Topic topic = mockTopic("topic1", "comment", ImmutableMap.of("key1", "value1"));
    assertUpdateTopic(new TopicUpdatesRequest(ImmutableList.of(req)), topic);
  }

  @Test
  public void testRemoveTopicProperties() {
    TopicUpdateRequest req = new TopicUpdateRequest.RemoveTopicPropertyRequest("key1");
    Topic topic = mockTopic("topic1", "comment", ImmutableMap.of());
    assertUpdateTopic(new TopicUpdatesRequest(ImmutableList.of(req)), topic);
  }

  @Test
  public void testUpdateTopicComment() {
    TopicUpdateRequest req = new TopicUpdateRequest.UpdateTopicCommentRequest("new comment");
    Topic topic = mockTopic("topic1", "new comment", ImmutableMap.of());
    assertUpdateTopic(new TopicUpdatesRequest(ImmutableList.of(req)), topic);
  }

  @Test
  public void testMultiUpdateRequest() {
    TopicUpdateRequest req1 = new TopicUpdateRequest.UpdateTopicCommentRequest("new comment");
    TopicUpdateRequest req2 = new TopicUpdateRequest.SetTopicPropertyRequest("key1", "value1");
    // update key1=value2
    TopicUpdateRequest req3 = new TopicUpdateRequest.SetTopicPropertyRequest("key1", "value2");
    TopicUpdateRequest req4 = new TopicUpdateRequest.SetTopicPropertyRequest("key2", "value2");
    // remove key2
    TopicUpdateRequest req5 = new TopicUpdateRequest.RemoveTopicPropertyRequest("key2");

    Topic topic = mockTopic("topic1", "new comment", ImmutableMap.of("key1", "value1"));
    assertUpdateTopic(
        new TopicUpdatesRequest(ImmutableList.of(req1, req2, req3, req4, req5)), topic);
  }

  @Test
  public void testDropTopic() {
    when(dispatcher.dropTopic(any())).thenReturn(true);
    Response resp =
        target(topicPath(metalake, catalog, schema) + "/topic1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    // test dropTopic return false
    when(dispatcher.dropTopic(any())).thenReturn(false);
    Response resp1 =
        target(topicPath(metalake, catalog, schema) + "/topic1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    DropResponse dropResp1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp1.getCode());
    Assertions.assertFalse(dropResp1.dropped());

    // test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).dropTopic(any());
    Response resp2 =
        target(topicPath(metalake, catalog, schema) + "/topic1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  private void assertUpdateTopic(TopicUpdatesRequest req, Topic updatedTopic) {
    when(dispatcher.alterTopic(any(), any(TopicChange.class))).thenReturn(updatedTopic);

    Response resp1 =
        target(topicPath(metalake, catalog, schema) + "/topic1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    TopicResponse topicResp = resp1.readEntity(TopicResponse.class);
    Assertions.assertEquals(0, topicResp.getCode());

    TopicDTO topicDTO = topicResp.getTopic();
    Assertions.assertEquals(updatedTopic.name(), topicDTO.name());
    Assertions.assertEquals(updatedTopic.comment(), topicDTO.comment());
    Assertions.assertEquals(updatedTopic.properties(), topicDTO.properties());
  }

  private Topic mockTopic(String name, String comment, Map<String, String> properties) {
    Topic mockedTopic = mock(Topic.class);
    when(mockedTopic.name()).thenReturn(name);
    when(mockedTopic.comment()).thenReturn(comment);
    when(mockedTopic.properties()).thenReturn(properties);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockedTopic.auditInfo()).thenReturn(mockAudit);

    return mockedTopic;
  }

  private String topicPath(String metalake, String catalog, String schema) {
    return "/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/" + schema + "/topics";
  }
}
