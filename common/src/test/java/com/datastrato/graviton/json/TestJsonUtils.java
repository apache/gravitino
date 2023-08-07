/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.json;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJsonUtils {

  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    objectMapper = JsonUtils.objectMapper();
  }

  @Test
  void testGetString() throws Exception {
    String json = "{\"property\": \"value\"}";
    JsonNode node = objectMapper.readTree(json);

    String result = JsonUtils.getString("property", node);
    assertEquals("value", result);
  }

  @Test
  void testGetStringListOrNull() throws Exception {
    String json = "{\"property\": [\"value1\", \"value2\"]}";
    JsonNode node = objectMapper.readTree(json);

    List<String> result = JsonUtils.getStringListOrNull("property", node);
    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("value1", result.get(0));
    assertEquals("value2", result.get(1));

    result = JsonUtils.getStringListOrNull("unknown", node);
    assertNull(result);

    result = JsonUtils.getStringListOrNull("unknown", node);
    assertNull(result);
  }
}
