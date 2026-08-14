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
import org.apache.gravitino.tag.TagValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagValuesAssociateRequest {

  @Test
  public void testTagValuesAssociateRequestSerDe() throws JsonProcessingException {
    TagValuesAssociateRequest request =
        new TagValuesAssociateRequest(
            new TagValue[] {TagValue.noValue("pii"), TagValue.of("data_domain", "finance")},
            new TagValue[] {TagValue.of("data_domain", "old")});

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    TagValuesAssociateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, TagValuesAssociateRequest.class);

    Assertions.assertEquals(request, deserialized);
    Assertions.assertArrayEquals(request.tagValuesToAdd(), deserialized.tagValuesToAdd());
    Assertions.assertArrayEquals(request.tagValuesToRemove(), deserialized.tagValuesToRemove());
  }

  @Test
  public void testTagValuesAssociateRequestValidateValues() throws JsonProcessingException {
    TagValuesAssociateRequest validRequest =
        new TagValuesAssociateRequest(new TagValue[] {TagValue.of("data_domain", "finance")}, null);
    Assertions.assertDoesNotThrow(validRequest::validate);
    Assertions.assertArrayEquals(new TagValue[0], validRequest.tagValuesToRemove());

    TagValuesAssociateRequest emptyRequest = new TagValuesAssociateRequest(null, null);
    Assertions.assertThrows(IllegalArgumentException.class, emptyRequest::validate);

    TagValuesAssociateRequest nullFieldRequest =
        JsonUtils.objectMapper()
            .readValue(
                "{\"tagsToAdd\":[{\"name\":\"data_domain\"}],\"tagsToRemove\":null}",
                TagValuesAssociateRequest.class);
    Assertions.assertDoesNotThrow(nullFieldRequest::validate);
    Assertions.assertArrayEquals(new TagValue[0], nullFieldRequest.tagValuesToRemove());

    TagValuesAssociateRequest nullFieldsRequest =
        JsonUtils.objectMapper()
            .readValue(
                "{\"tagsToAdd\":null,\"tagsToRemove\":null}", TagValuesAssociateRequest.class);
    Assertions.assertArrayEquals(new TagValue[0], nullFieldsRequest.tagValuesToAdd());
    Assertions.assertArrayEquals(new TagValue[0], nullFieldsRequest.tagValuesToRemove());
    Assertions.assertThrows(IllegalArgumentException.class, nullFieldsRequest::validate);

    TagValuesAssociateRequest blankNameRequest =
        JsonUtils.objectMapper()
            .readValue(
                "{\"tagsToAdd\":[{\"name\":\" \",\"value\":\"finance\"}]}",
                TagValuesAssociateRequest.class);
    Assertions.assertThrows(IllegalArgumentException.class, blankNameRequest::validate);

    TagValuesAssociateRequest blankValueRequest =
        JsonUtils.objectMapper()
            .readValue(
                "{\"tagsToAdd\":[{\"name\":\"data_domain\",\"value\":\" \"}]}",
                TagValuesAssociateRequest.class);
    Assertions.assertThrows(IllegalArgumentException.class, blankValueRequest::validate);
  }

  @Test
  public void testTagValuesAssociateRequestRejectsOverlappingPairs() {
    TagValuesAssociateRequest exactPairOverlap =
        new TagValuesAssociateRequest(
            new TagValue[] {TagValue.of("data_domain", "finance"), TagValue.noValue("owner")},
            new TagValue[] {TagValue.of("data_domain", "finance")});
    Assertions.assertThrows(IllegalArgumentException.class, exactPairOverlap::validate);

    TagValuesAssociateRequest noValueOverlap =
        new TagValuesAssociateRequest(
            new TagValue[] {TagValue.noValue("owner")}, new TagValue[] {TagValue.noValue("owner")});
    Assertions.assertThrows(IllegalArgumentException.class, noValueOverlap::validate);

    TagValuesAssociateRequest differentValue =
        new TagValuesAssociateRequest(
            new TagValue[] {TagValue.of("data_domain", "finance")},
            new TagValue[] {TagValue.of("data_domain", "risk")});
    Assertions.assertDoesNotThrow(differentValue::validate);
  }

  @Test
  public void testTagValuesAssociateRequestRejectsV1Shape() {
    Assertions.assertThrows(
        JsonProcessingException.class,
        () ->
            JsonUtils.objectMapper()
                .readValue("{\"tagsToAdd\":[\"data_domain\"]}", TagValuesAssociateRequest.class));
  }
}
