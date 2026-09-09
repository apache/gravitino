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
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.junit.jupiter.api.Test;

public class TestObjectMapperProvider {

  @Test
  public void testGetContext() {
    ObjectMapperProvider provider = new ObjectMapperProvider();
    Class<Object> someClass = Object.class;

    ObjectMapper objectMapper = provider.getContext(someClass);

    assertNotNull(objectMapper);
    assertEquals(
        JsonInclude.Include.NON_NULL,
        objectMapper.getSerializationConfig().getDefaultPropertyInclusion().getValueInclusion());
  }

  @Test
  public void testErrorResponseStackIsRedactedOnSerialization() throws JsonProcessingException {
    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
    ErrorResponse errorResponse =
        ErrorResponse.internalError(
            "public error message", new RuntimeException("private error details"));

    assertNotNull(errorResponse.getStack());
    assertTrue(
        errorResponse.getStack().stream().anyMatch(line -> line.contains("private error details")));

    JsonNode responseJson = objectMapper.readTree(objectMapper.writeValueAsString(errorResponse));
    assertEquals("public error message", responseJson.get("message").asText());
    assertFalse(responseJson.has("stack"));
  }

  @Test
  public void testErrorResponseStackIsAcceptedOnDeserialization() throws JsonProcessingException {
    ObjectMapper serverObjectMapper = ObjectMapperProvider.objectMapper();
    ObjectMapper clientObjectMapper = new ObjectMapper();
    ErrorResponse errorResponse =
        ErrorResponse.internalError(
            "public error message", new RuntimeException("private error details"));
    String legacyResponseJson = clientObjectMapper.writeValueAsString(errorResponse);

    assertTrue(clientObjectMapper.readTree(legacyResponseJson).has("stack"));

    ErrorResponse deserialized =
        serverObjectMapper.readValue(legacyResponseJson, ErrorResponse.class);
    assertEquals(errorResponse.getStack(), deserialized.getStack());
  }
}
