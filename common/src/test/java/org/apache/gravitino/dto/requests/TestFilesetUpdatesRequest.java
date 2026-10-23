/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilesetUpdatesRequest {

  @Test
  void testUpdateTypeSerialization() throws JsonProcessingException {
    for (Fileset.Type type : Fileset.Type.values()) {
      FilesetUpdateRequest update = new FilesetUpdateRequest.UpdateFilesetTypeRequest(type);
      FilesetUpdatesRequest request = new FilesetUpdatesRequest(Collections.singletonList(update));
      request.validate();
      String json = JsonUtils.objectMapper().writeValueAsString(request);
      Assertions.assertEquals(
          "updateType", JsonUtils.objectMapper().readTree(json).at("/updates/0/@type").asText());
      Assertions.assertEquals(
          type.name().toLowerCase(Locale.ROOT),
          JsonUtils.objectMapper().readTree(json).at("/updates/0/newType").asText());
      FilesetUpdatesRequest decoded =
          JsonUtils.objectMapper().readValue(json, FilesetUpdatesRequest.class);
      decoded.validate();
      Assertions.assertEquals(1, decoded.getUpdates().size());
      Assertions.assertEquals(
          FilesetChange.updateType(type), decoded.getUpdates().get(0).filesetChange());
    }
  }

  @Test
  void testUpdateTypeValidation() throws JsonProcessingException {
    for (String update :
        new String[] {
          "{\"@type\":\"updateType\"}", "{\"@type\":\"updateType\",\"newType\":null}"
        }) {
      FilesetUpdateRequest request =
          JsonUtils.objectMapper().readValue(update, FilesetUpdateRequest.class);
      Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    }
    Assertions.assertThrows(
        JsonProcessingException.class,
        () ->
            JsonUtils.objectMapper()
                .readValue(
                    "{\"@type\":\"updateType\",\"newType\":\"unknown\"}",
                    FilesetUpdateRequest.class));
  }

  @Test
  public void testValidateWithNullUpdates() {
    // Test that creating a request with null updates throws an exception during validation
    FilesetUpdatesRequest request = new FilesetUpdatesRequest(null);

    final IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);

    Assertions.assertEquals("Updates list cannot be null", exception.getMessage());
  }

  @Test
  public void testValidateWithEmptyUpdates() {
    // Test that a non-null but empty list of updates is valid
    FilesetUpdatesRequest request = new FilesetUpdatesRequest(Collections.emptyList());

    Assertions.assertDoesNotThrow(request::validate);
  }
}
