/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagUpdatesRequest {

  @Test
  public void testRenameTagRequestSerDe() throws JsonProcessingException {
    TagUpdateRequest.RenameTagRequest request =
        new TagUpdateRequest.RenameTagRequest("tag_test_new");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    TagUpdateRequest.RenameTagRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, TagUpdateRequest.RenameTagRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("tag_test_new", deserRequest.getNewName());
  }

  @Test
  public void testUpdateTagCommentRequestSerDe() throws JsonProcessingException {
    TagUpdateRequest.UpdateTagCommentRequest request =
        new TagUpdateRequest.UpdateTagCommentRequest("tag comment new");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    TagUpdateRequest.UpdateTagCommentRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, TagUpdateRequest.UpdateTagCommentRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("tag comment new", deserRequest.getNewComment());
  }

  @Test
  public void testSetTagPropertyRequestSerDe() throws JsonProcessingException {
    TagUpdateRequest.SetTagPropertyRequest request =
        new TagUpdateRequest.SetTagPropertyRequest("key", "value");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    TagUpdateRequest.SetTagPropertyRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, TagUpdateRequest.SetTagPropertyRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("key", deserRequest.getProperty());
    Assertions.assertEquals("value", deserRequest.getValue());
  }

  @Test
  public void testRemoveTagPropertyRequestSerDe() throws JsonProcessingException {
    TagUpdateRequest.RemoveTagPropertyRequest request =
        new TagUpdateRequest.RemoveTagPropertyRequest("key");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    TagUpdateRequest.RemoveTagPropertyRequest deserRequest =
        JsonUtils.objectMapper()
            .readValue(serJson, TagUpdateRequest.RemoveTagPropertyRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("key", deserRequest.getProperty());
  }

  @Test
  public void testTagUpdatesRequestSerDe() throws JsonProcessingException {
    TagUpdateRequest request = new TagUpdateRequest.RenameTagRequest("tag_test_new");
    TagUpdateRequest request1 = new TagUpdateRequest.UpdateTagCommentRequest("tag comment new");
    TagUpdateRequest request2 = new TagUpdateRequest.SetTagPropertyRequest("key", "value");
    TagUpdateRequest request3 = new TagUpdateRequest.RemoveTagPropertyRequest("key");

    List<TagUpdateRequest> updates = ImmutableList.of(request, request1, request2, request3);
    TagUpdatesRequest tagUpdatesRequest = new TagUpdatesRequest(updates);
    String serJson = JsonUtils.objectMapper().writeValueAsString(tagUpdatesRequest);
    TagUpdatesRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, TagUpdatesRequest.class);
    Assertions.assertEquals(tagUpdatesRequest, deserRequest);
    Assertions.assertEquals(updates, deserRequest.getUpdates());
  }
}
