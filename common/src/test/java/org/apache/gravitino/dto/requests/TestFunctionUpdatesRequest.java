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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFunctionUpdatesRequest {

  @Test
  public void testFunctionUpdatesRequestSerDe() throws JsonProcessingException {
    FunctionUpdateRequest.UpdateCommentRequest updateComment =
        new FunctionUpdateRequest.UpdateCommentRequest("new comment");
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder().withParameters(new FunctionParamDTO[0]).build();
    FunctionUpdateRequest.AddDefinitionRequest addDefinition =
        new FunctionUpdateRequest.AddDefinitionRequest(definition);

    FunctionUpdatesRequest request =
        new FunctionUpdatesRequest(Arrays.asList(updateComment, addDefinition));

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    FunctionUpdatesRequest deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionUpdatesRequest.class);

    Assertions.assertEquals(request, deserialized);
    Assertions.assertEquals(2, deserialized.getUpdates().size());
  }

  @Test
  public void testValidate() {
    FunctionUpdateRequest.UpdateCommentRequest updateComment =
        new FunctionUpdateRequest.UpdateCommentRequest("comment");

    // Valid request
    FunctionUpdatesRequest validRequest =
        new FunctionUpdatesRequest(Collections.singletonList(updateComment));
    Assertions.assertDoesNotThrow(validRequest::validate);

    // Empty list is valid
    FunctionUpdatesRequest emptyRequest = new FunctionUpdatesRequest(Collections.emptyList());
    Assertions.assertDoesNotThrow(emptyRequest::validate);

    // Null list is invalid
    FunctionUpdatesRequest invalidRequest = new FunctionUpdatesRequest(null);
    Assertions.assertThrows(IllegalArgumentException.class, invalidRequest::validate);
  }

  @Test
  public void testValidateWithInvalidUpdate() {
    // Request with invalid inner request
    FunctionUpdateRequest.AddDefinitionRequest invalidInner =
        new FunctionUpdateRequest.AddDefinitionRequest(null);

    FunctionUpdatesRequest request =
        new FunctionUpdatesRequest(Collections.singletonList(invalidInner));

    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }
}
