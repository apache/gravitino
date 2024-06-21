/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagCreateRequest {

  @Test
  public void testTagCreateRequestSerDe() throws JsonProcessingException {
    TagCreateRequest request = new TagCreateRequest("tag_test", "tag comment", null);
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    TagCreateRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, TagCreateRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("tag_test", deserRequest.getName());
    Assertions.assertEquals("tag comment", deserRequest.getComment());
    Assertions.assertNull(deserRequest.getProperties());

    Map<String, String> properties = ImmutableMap.of("key", "value");
    TagCreateRequest request1 = new TagCreateRequest("tag_test", "tag comment", properties);
    serJson = JsonUtils.objectMapper().writeValueAsString(request1);
    TagCreateRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson, TagCreateRequest.class);
    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertEquals(properties, deserRequest1.getProperties());
  }
}
