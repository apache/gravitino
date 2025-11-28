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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.TagCreateRequest;
import org.apache.gravitino.dto.requests.TagUpdateRequest;
import org.apache.gravitino.dto.requests.TagUpdatesRequest;
import org.apache.gravitino.dto.requests.TagsAssociateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetadataObjectListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TagListResponse;
import org.apache.gravitino.dto.responses.TagResponse;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.tag.TagManager;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagOperations extends BaseOperationsTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private TagManager tagManager = mock(TagManager.class);

  private String metalake = "test_metalake";

  private AuditInfo testAuditInfo1 =
      AuditInfo.builder().withCreator("user1").withCreateTime(Instant.now()).build();

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(TagOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(tagManager).to(TagDispatcher.class).ranked(2);
            bindFactory(TestTagOperations.MockServletRequestFactory.class)
                .to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListTags() {
    String[] tags = new String[] {"tag1", "tag2"};
    when(tagManager.listTags(metalake)).thenReturn(tags);

    Response response =
        target(tagPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    NameListResponse nameListResponse = response.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse.getCode());
    Assertions.assertArrayEquals(tags, nameListResponse.getNames());

    when(tagManager.listTags(metalake)).thenReturn(null);
    Response resp1 =
        target(tagPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    NameListResponse nameListResponse1 = resp1.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse1.getCode());
    Assertions.assertEquals(0, nameListResponse1.getNames().length);

    when(tagManager.listTags(metalake)).thenReturn(new String[0]);
    Response resp2 =
        target(tagPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());

    NameListResponse nameListResponse2 = resp2.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse2.getCode());
    Assertions.assertEquals(0, nameListResponse2.getNames().length);

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(tagManager).listTags(metalake);
    Response resp3 =
        target(tagPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(tagManager).listTags(metalake);
    Response resp4 =
        target(tagPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResp1 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testListTagsInfo() {
    TagEntity tag1 =
        TagEntity.builder()
            .withName("tag1")
            .withId(1L)
            .withComment("tag1 comment")
            .withAuditInfo(testAuditInfo1)
            .build();

    TagEntity tag2 =
        TagEntity.builder()
            .withName("tag2")
            .withId(1L)
            .withComment("tag2 comment")
            .withAuditInfo(testAuditInfo1)
            .build();

    Tag[] tags = new Tag[] {tag1, tag2};
    when(tagManager.listTagsInfo(metalake)).thenReturn(tags);

    Response resp =
        target(tagPath(metalake))
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    TagListResponse tagListResp = resp.readEntity(TagListResponse.class);
    Assertions.assertEquals(0, tagListResp.getCode());
    Assertions.assertEquals(tags.length, tagListResp.getTags().length);

    Assertions.assertEquals(tag1.name(), tagListResp.getTags()[0].name());
    Assertions.assertEquals(tag1.comment(), tagListResp.getTags()[0].comment());
    Assertions.assertEquals(Optional.empty(), tagListResp.getTags()[0].inherited());

    Assertions.assertEquals(tag2.name(), tagListResp.getTags()[1].name());
    Assertions.assertEquals(tag2.comment(), tagListResp.getTags()[1].comment());
    Assertions.assertEquals(Optional.empty(), tagListResp.getTags()[1].inherited());

    // Test return null
    when(tagManager.listTagsInfo(metalake)).thenReturn(null);
    Response resp1 =
        target(tagPath(metalake))
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    TagListResponse tagListResp1 = resp1.readEntity(TagListResponse.class);
    Assertions.assertEquals(0, tagListResp1.getCode());
    Assertions.assertEquals(0, tagListResp1.getTags().length);

    // Test return empty array
    when(tagManager.listTagsInfo(metalake)).thenReturn(new Tag[0]);
    Response resp2 =
        target(tagPath(metalake))
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());

    TagListResponse tagListResp2 = resp2.readEntity(TagListResponse.class);
    Assertions.assertEquals(0, tagListResp2.getCode());
    Assertions.assertEquals(0, tagListResp2.getTags().length);
  }

  @Test
  public void testCreateTag() {
    TagEntity tag1 =
        TagEntity.builder()
            .withName("tag1")
            .withId(1L)
            .withComment("tag1 comment")
            .withAuditInfo(testAuditInfo1)
            .build();
    when(tagManager.createTag(metalake, "tag1", "tag1 comment", null)).thenReturn(tag1);

    TagCreateRequest request = new TagCreateRequest("tag1", "tag1 comment", null);
    Response resp =
        target(tagPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    TagResponse tagResp = resp.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResp.getCode());

    Tag respTag = tagResp.getTag();
    Assertions.assertEquals(tag1.name(), respTag.name());
    Assertions.assertEquals(tag1.comment(), respTag.comment());
    Assertions.assertEquals(Optional.empty(), respTag.inherited());

    // Test throw TagAlreadyExistsException
    doThrow(new TagAlreadyExistsException("mock error"))
        .when(tagManager)
        .createTag(any(), any(), any(), any());
    Response resp1 =
        target(tagPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp.getCode());
    Assertions.assertEquals(TagAlreadyExistsException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(tagManager)
        .createTag(any(), any(), any(), any());

    Response resp2 =
        target(tagPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testGetTag() {
    TagEntity tag1 =
        TagEntity.builder()
            .withName("tag1")
            .withId(1L)
            .withComment("tag1 comment")
            .withAuditInfo(testAuditInfo1)
            .build();
    when(tagManager.getTag(metalake, "tag1")).thenReturn(tag1);

    Response resp =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    TagResponse tagResp = resp.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResp.getCode());

    Tag respTag = tagResp.getTag();
    Assertions.assertEquals(tag1.name(), respTag.name());
    Assertions.assertEquals(tag1.comment(), respTag.comment());
    Assertions.assertEquals(Optional.empty(), respTag.inherited());

    // Test throw NoSuchTagException
    doThrow(new NoSuchTagException("mock error")).when(tagManager).getTag(metalake, "tag1");

    Response resp2 =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchTagException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(tagManager).getTag(metalake, "tag1");

    Response resp3 =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp1 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testAlterTag() {
    TagEntity newTag =
        TagEntity.builder()
            .withName("new_tag1")
            .withId(1L)
            .withComment("new tag1 comment")
            .withAuditInfo(testAuditInfo1)
            .build();

    TagChange[] changes =
        new TagChange[] {TagChange.rename("new_tag1"), TagChange.updateComment("new tag1 comment")};

    when(tagManager.alterTag(metalake, "tag1", changes)).thenReturn(newTag);

    TagUpdateRequest[] requests =
        new TagUpdateRequest[] {
          new TagUpdateRequest.RenameTagRequest("new_tag1"),
          new TagUpdateRequest.UpdateTagCommentRequest("new tag1 comment")
        };
    TagUpdatesRequest request = new TagUpdatesRequest(Lists.newArrayList(requests));
    Response resp =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    TagResponse tagResp = resp.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResp.getCode());

    Tag respTag = tagResp.getTag();
    Assertions.assertEquals(newTag.name(), respTag.name());
    Assertions.assertEquals(newTag.comment(), respTag.comment());
    Assertions.assertEquals(Optional.empty(), respTag.inherited());

    // Test throw NoSuchTagException
    doThrow(new NoSuchTagException("mock error")).when(tagManager).alterTag(any(), any(), any());

    Response resp1 =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchTagException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(tagManager).alterTag(any(), any(), any());

    Response resp2 =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testDeleteTag() {
    when(tagManager.deleteTag(metalake, "tag1")).thenReturn(true);

    Response resp =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    when(tagManager.deleteTag(metalake, "tag1")).thenReturn(false);
    Response resp1 =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    DropResponse dropResp1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp1.getCode());
    Assertions.assertFalse(dropResp1.dropped());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(tagManager).deleteTag(any(), any());

    Response resp2 =
        target(tagPath(metalake))
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testListTagsForObject() {
    MetadataObject catalog = MetadataObjects.parse("object1", MetadataObject.Type.CATALOG);
    MetadataObject schema = MetadataObjects.parse("object1.object2", MetadataObject.Type.SCHEMA);
    MetadataObject table =
        MetadataObjects.parse("object1.object2.object3", MetadataObject.Type.TABLE);
    MetadataObject column =
        MetadataObjects.parse("object1.object2.object3.object4", MetadataObject.Type.COLUMN);

    Tag[] catalogTagInfos =
        new Tag[] {
          TagEntity.builder().withName("tag1").withId(1L).withAuditInfo(testAuditInfo1).build()
        };
    when(tagManager.listTagsInfoForMetadataObject(metalake, catalog)).thenReturn(catalogTagInfos);

    Tag[] schemaTagInfos =
        new Tag[] {
          TagEntity.builder().withName("tag3").withId(1L).withAuditInfo(testAuditInfo1).build()
        };
    when(tagManager.listTagsInfoForMetadataObject(metalake, schema)).thenReturn(schemaTagInfos);

    Tag[] tableTagInfos =
        new Tag[] {
          TagEntity.builder().withName("tag5").withId(1L).withAuditInfo(testAuditInfo1).build()
        };
    when(tagManager.listTagsInfoForMetadataObject(metalake, table)).thenReturn(tableTagInfos);

    Tag[] columnTagInfos =
        new Tag[] {
          TagEntity.builder().withName("tag7").withId(1L).withAuditInfo(testAuditInfo1).build()
        };
    when(tagManager.listTagsInfoForMetadataObject(metalake, column)).thenReturn(columnTagInfos);

    // Test catalog tags
    Response response =
        target(tagPath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    TagListResponse tagListResponse = response.readEntity(TagListResponse.class);
    Assertions.assertEquals(0, tagListResponse.getCode());
    Assertions.assertEquals(catalogTagInfos.length, tagListResponse.getTags().length);

    Map<String, Tag> resultTags =
        Arrays.stream(tagListResponse.getTags())
            .collect(Collectors.toMap(Tag::name, Function.identity()));

    Assertions.assertTrue(resultTags.containsKey("tag1"));
    Assertions.assertFalse(resultTags.get("tag1").inherited().get());

    Response response1 =
        target(tagPath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());

    NameListResponse nameListResponse = response1.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse.getCode());
    Assertions.assertEquals(catalogTagInfos.length, nameListResponse.getNames().length);
    Assertions.assertArrayEquals(
        Arrays.stream(catalogTagInfos).map(Tag::name).toArray(String[]::new),
        nameListResponse.getNames());

    // Test schema tags
    Response response2 =
        target(tagPath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response2.getStatus());

    TagListResponse tagListResponse1 = response2.readEntity(TagListResponse.class);
    Assertions.assertEquals(0, tagListResponse1.getCode());
    Assertions.assertEquals(
        schemaTagInfos.length + catalogTagInfos.length, tagListResponse1.getTags().length);

    Map<String, Tag> resultTags1 =
        Arrays.stream(tagListResponse1.getTags())
            .collect(Collectors.toMap(Tag::name, Function.identity()));

    Assertions.assertTrue(resultTags1.containsKey("tag1"));
    Assertions.assertTrue(resultTags1.containsKey("tag3"));

    Assertions.assertTrue(resultTags1.get("tag1").inherited().get());
    Assertions.assertFalse(resultTags1.get("tag3").inherited().get());

    Response response3 =
        target(tagPath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response3.getStatus());

    NameListResponse nameListResponse1 = response3.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse1.getCode());
    Assertions.assertEquals(
        schemaTagInfos.length + catalogTagInfos.length, nameListResponse1.getNames().length);
    Set<String> resultNames = Sets.newHashSet(nameListResponse1.getNames());
    Assertions.assertTrue(resultNames.contains("tag1"));
    Assertions.assertTrue(resultNames.contains("tag3"));

    // Test table tags
    Response response4 =
        target(tagPath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response4.getStatus());

    TagListResponse tagListResponse2 = response4.readEntity(TagListResponse.class);
    Assertions.assertEquals(0, tagListResponse2.getCode());
    Assertions.assertEquals(
        schemaTagInfos.length + catalogTagInfos.length + tableTagInfos.length,
        tagListResponse2.getTags().length);

    Map<String, Tag> resultTags2 =
        Arrays.stream(tagListResponse2.getTags())
            .collect(Collectors.toMap(Tag::name, Function.identity()));

    Assertions.assertTrue(resultTags2.containsKey("tag1"));
    Assertions.assertTrue(resultTags2.containsKey("tag3"));
    Assertions.assertTrue(resultTags2.containsKey("tag5"));

    Assertions.assertTrue(resultTags2.get("tag1").inherited().get());
    Assertions.assertTrue(resultTags2.get("tag3").inherited().get());
    Assertions.assertFalse(resultTags2.get("tag5").inherited().get());

    Response response5 =
        target(tagPath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response5.getStatus());

    NameListResponse nameListResponse2 = response5.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse2.getCode());
    Assertions.assertEquals(
        schemaTagInfos.length + catalogTagInfos.length + tableTagInfos.length,
        nameListResponse2.getNames().length);

    Set<String> resultNames1 = Sets.newHashSet(nameListResponse2.getNames());
    Assertions.assertTrue(resultNames1.contains("tag1"));
    Assertions.assertTrue(resultNames1.contains("tag3"));
    Assertions.assertTrue(resultNames1.contains("tag5"));

    // Test column tags
    Response response6 =
        target(tagPath(metalake))
            .path(column.type().toString())
            .path(column.fullName())
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response6.getStatus());

    TagListResponse tagListResponse3 = response6.readEntity(TagListResponse.class);
    Assertions.assertEquals(0, tagListResponse3.getCode());
    Assertions.assertEquals(
        schemaTagInfos.length
            + catalogTagInfos.length
            + tableTagInfos.length
            + columnTagInfos.length,
        tagListResponse3.getTags().length);

    Map<String, Tag> resultTags3 =
        Arrays.stream(tagListResponse3.getTags())
            .collect(Collectors.toMap(Tag::name, Function.identity()));

    Assertions.assertTrue(resultTags3.containsKey("tag1"));
    Assertions.assertTrue(resultTags3.containsKey("tag3"));
    Assertions.assertTrue(resultTags3.containsKey("tag5"));
    Assertions.assertTrue(resultTags3.containsKey("tag7"));

    Assertions.assertTrue(resultTags3.get("tag1").inherited().get());
    Assertions.assertTrue(resultTags3.get("tag3").inherited().get());
    Assertions.assertTrue(resultTags3.get("tag5").inherited().get());
    Assertions.assertFalse(resultTags3.get("tag7").inherited().get());

    Response response7 =
        target(tagPath(metalake))
            .path(column.type().toString())
            .path(column.fullName())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response7.getStatus());

    NameListResponse nameListResponse3 = response7.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse3.getCode());

    Assertions.assertEquals(
        schemaTagInfos.length
            + catalogTagInfos.length
            + tableTagInfos.length
            + columnTagInfos.length,
        nameListResponse3.getNames().length);

    Set<String> resultNames2 = Sets.newHashSet(nameListResponse3.getNames());
    Assertions.assertTrue(resultNames2.contains("tag1"));
    Assertions.assertTrue(resultNames2.contains("tag3"));
    Assertions.assertTrue(resultNames2.contains("tag5"));
    Assertions.assertTrue(resultNames2.contains("tag7"));
  }

  @Test
  public void testGetTagForObject() {
    TagEntity tag1 =
        TagEntity.builder().withName("tag1").withId(1L).withAuditInfo(testAuditInfo1).build();
    MetadataObject catalog = MetadataObjects.parse("object1", MetadataObject.Type.CATALOG);
    when(tagManager.getTagForMetadataObject(metalake, catalog, "tag1")).thenReturn(tag1);

    TagEntity tag2 =
        TagEntity.builder().withName("tag2").withId(1L).withAuditInfo(testAuditInfo1).build();
    MetadataObject schema = MetadataObjects.parse("object1.object2", MetadataObject.Type.SCHEMA);
    when(tagManager.getTagForMetadataObject(metalake, schema, "tag2")).thenReturn(tag2);

    TagEntity tag3 =
        TagEntity.builder().withName("tag3").withId(1L).withAuditInfo(testAuditInfo1).build();
    MetadataObject table =
        MetadataObjects.parse("object1.object2.object3", MetadataObject.Type.TABLE);
    when(tagManager.getTagForMetadataObject(metalake, table, "tag3")).thenReturn(tag3);

    TagEntity tag4 =
        TagEntity.builder().withName("tag4").withId(1L).withAuditInfo(testAuditInfo1).build();
    MetadataObject column =
        MetadataObjects.parse("object1.object2.object3.object4", MetadataObject.Type.COLUMN);
    when(tagManager.getTagForMetadataObject(metalake, column, "tag4")).thenReturn(tag4);

    // Test catalog tag
    Response response =
        target(tagPath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    TagResponse tagResponse = response.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResponse.getCode());

    Tag respTag = tagResponse.getTag();
    Assertions.assertEquals(tag1.name(), respTag.name());
    Assertions.assertEquals(tag1.comment(), respTag.comment());
    Assertions.assertFalse(respTag.inherited().get());

    // Test schema tag
    Response response1 =
        target(tagPath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .path("tag2")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());

    TagResponse tagResponse1 = response1.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResponse1.getCode());

    Tag respTag1 = tagResponse1.getTag();
    Assertions.assertEquals(tag2.name(), respTag1.name());
    Assertions.assertEquals(tag2.comment(), respTag1.comment());
    Assertions.assertFalse(respTag1.inherited().get());

    // Test table tag
    Response response2 =
        target(tagPath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("tag3")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response2.getStatus());

    TagResponse tagResponse2 = response2.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResponse2.getCode());

    Tag respTag2 = tagResponse2.getTag();
    Assertions.assertEquals(tag3.name(), respTag2.name());
    Assertions.assertEquals(tag3.comment(), respTag2.comment());
    Assertions.assertFalse(respTag2.inherited().get());

    // Test column tag
    Response response3 =
        target(tagPath(metalake))
            .path(column.type().toString())
            .path(column.fullName())
            .path("tag4")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response3.getStatus());

    TagResponse tagResponse3 = response3.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResponse3.getCode());

    Tag respTag3 = tagResponse3.getTag();
    Assertions.assertEquals(tag4.name(), respTag3.name());
    Assertions.assertEquals(tag4.comment(), respTag3.comment());
    Assertions.assertFalse(respTag3.inherited().get());

    // Test get schema inherited tag
    Response response4 =
        target(tagPath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .path("tag1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response4.getStatus());

    TagResponse tagResponse4 = response4.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResponse4.getCode());

    Tag respTag4 = tagResponse4.getTag();
    Assertions.assertEquals(tag1.name(), respTag4.name());
    Assertions.assertEquals(tag1.comment(), respTag4.comment());
    Assertions.assertTrue(respTag4.inherited().get());

    // Test get table inherited tag
    Response response5 =
        target(tagPath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("tag2")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response5.getStatus());

    TagResponse tagResponse5 = response5.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResponse5.getCode());

    Tag respTag5 = tagResponse5.getTag();
    Assertions.assertEquals(tag2.name(), respTag5.name());
    Assertions.assertEquals(tag2.comment(), respTag5.comment());
    Assertions.assertTrue(respTag5.inherited().get());

    // Test get column inherited tag
    Response response6 =
        target(tagPath(metalake))
            .path(column.type().toString())
            .path(column.fullName())
            .path("tag3")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response6.getStatus());

    TagResponse tagResponse6 = response6.readEntity(TagResponse.class);
    Assertions.assertEquals(0, tagResponse6.getCode());

    Tag respTag6 = tagResponse6.getTag();
    Assertions.assertEquals(tag3.name(), respTag6.name());
    Assertions.assertEquals(tag3.comment(), respTag6.comment());
    Assertions.assertTrue(respTag6.inherited().get());

    // Test catalog tag throw NoSuchTagException
    Response response7 =
        target(tagPath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("tag2")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response7.getStatus());

    ErrorResponse errorResponse = response7.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchTagException.class.getSimpleName(), errorResponse.getType());

    // Test schema tag throw NoSuchTagException
    Response response8 =
        target(tagPath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .path("tag3")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response8.getStatus());

    ErrorResponse errorResponse1 = response8.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NoSuchTagException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testAssociateTagsForObject() {
    String[] tagsToAdd = new String[] {"tag1", "tag2"};
    String[] tagsToRemove = new String[] {"tag3", "tag4"};

    MetadataObject catalog = MetadataObjects.parse("object1", MetadataObject.Type.CATALOG);
    when(tagManager.associateTagsForMetadataObject(metalake, catalog, tagsToAdd, tagsToRemove))
        .thenReturn(tagsToAdd);

    TagsAssociateRequest request = new TagsAssociateRequest(tagsToAdd, tagsToRemove);
    Response response =
        target(tagPath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    NameListResponse nameListResponse = response.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse.getCode());

    Assertions.assertArrayEquals(tagsToAdd, nameListResponse.getNames());

    // Test throw null tags
    when(tagManager.associateTagsForMetadataObject(metalake, catalog, tagsToAdd, tagsToRemove))
        .thenReturn(null);
    Response response1 =
        target(tagPath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());

    NameListResponse nameListResponse1 = response1.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse1.getCode());

    Assertions.assertEquals(0, nameListResponse1.getNames().length);

    // Test throw TagAlreadyAssociatedException
    doThrow(new TagAlreadyAssociatedException("mock error"))
        .when(tagManager)
        .associateTagsForMetadataObject(metalake, catalog, tagsToAdd, tagsToRemove);
    Response response2 =
        target(tagPath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), response2.getStatus());

    ErrorResponse errorResponse = response2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        TagAlreadyAssociatedException.class.getSimpleName(), errorResponse.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(tagManager)
        .associateTagsForMetadataObject(any(), any(), any(), any());

    Response response3 =
        target(tagPath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response3.getStatus());

    ErrorResponse errorResponse1 = response3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testListMetadataObjectForTag() {
    MetadataObject[] objects =
        new MetadataObject[] {
          MetadataObjects.parse("object1", MetadataObject.Type.CATALOG),
          MetadataObjects.parse("object1.object2", MetadataObject.Type.SCHEMA),
          MetadataObjects.parse("object1.object2.object3", MetadataObject.Type.TABLE),
          MetadataObjects.parse("object1.object2.object3.object4", MetadataObject.Type.COLUMN)
        };

    when(tagManager.listMetadataObjectsForTag(metalake, "tag1")).thenReturn(objects);

    Response response =
        target(tagPath(metalake))
            .path("tag1")
            .path("objects")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    MetadataObjectListResponse objectListResponse =
        response.readEntity(MetadataObjectListResponse.class);
    Assertions.assertEquals(0, objectListResponse.getCode());

    MetadataObject[] respObjects = objectListResponse.getMetadataObjects();
    Assertions.assertEquals(objects.length, respObjects.length);

    for (int i = 0; i < objects.length; i++) {
      Assertions.assertEquals(objects[i].type(), respObjects[i].type());
      Assertions.assertEquals(objects[i].fullName(), respObjects[i].fullName());
    }

    // Test throw NoSuchTagException
    doThrow(new NoSuchTagException("mock error"))
        .when(tagManager)
        .listMetadataObjectsForTag(metalake, "tag1");

    Response response1 =
        target(tagPath(metalake))
            .path("tag1")
            .path("objects")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response1.getStatus());

    ErrorResponse errorResponse = response1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchTagException.class.getSimpleName(), errorResponse.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(tagManager)
        .listMetadataObjectsForTag(any(), any());

    Response response2 =
        target(tagPath(metalake))
            .path("tag1")
            .path("objects")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response2.getStatus());

    ErrorResponse errorResponse1 = response2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  private String tagPath(String metalake) {
    return "/metalakes/" + metalake + "/tags";
  }
}
