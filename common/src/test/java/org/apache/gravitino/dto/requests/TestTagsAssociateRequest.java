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
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.tag.TagValuePair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagsAssociateRequest {

  @Test
  public void testTagsAssociateRequestSerDeWithPairs() throws JsonProcessingException {
    TagsAssociateRequest request =
        new TagsAssociateRequest(
            new TagValuePair[] {
              TagValuePair.valueless("pii"), TagValuePair.of("data_domain", "finance")
            },
            new TagValuePair[] {TagValuePair.of("data_domain", "old")});

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    TagsAssociateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, TagsAssociateRequest.class);

    Assertions.assertEquals(request, deserialized);
    Assertions.assertArrayEquals(request.getTagsToAdd(), deserialized.getTagsToAdd());
    Assertions.assertArrayEquals(request.getTagsToRemove(), deserialized.getTagsToRemove());
  }

  @Test
  public void testTagsAssociateRequestValidatePairs() {
    TagsAssociateRequest validRequest =
        new TagsAssociateRequest(
            new TagValuePair[] {TagValuePair.of("data_domain", "finance")}, null);
    Assertions.assertDoesNotThrow(validRequest::validate);

    TagsAssociateRequest blankNameRequest =
        new TagsAssociateRequest(new TagValuePair[] {TagValuePair.of(" ", "finance")}, null);
    Assertions.assertThrows(IllegalArgumentException.class, blankNameRequest::validate);

    TagsAssociateRequest blankValueRequest =
        new TagsAssociateRequest(new TagValuePair[] {TagValuePair.of("data_domain", " ")}, null);
    Assertions.assertThrows(IllegalArgumentException.class, blankValueRequest::validate);
  }
}
