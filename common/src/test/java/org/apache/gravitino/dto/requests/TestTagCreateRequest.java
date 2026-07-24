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
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.json.JsonUtils;
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

  @Test
  public void testTagCreateRequestSerDeWithAllowedValues() throws JsonProcessingException {
    String[] allowedValues = new String[] {"finance", "risk"};
    TagCreateRequest request = new TagCreateRequest("tag_test", "tag comment", null, allowedValues);

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    TagCreateRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, TagCreateRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertArrayEquals(allowedValues, deserRequest.getAllowedValues());
  }

  @Test
  public void testTagCreateRequestValidateAllowedValues() {
    new TagCreateRequest("tag_test", "tag comment", null, new String[] {"finance"}).validate();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new TagCreateRequest("tag_test", null, null, new String[] {""}).validate());
    char[] tooLongValueChars = new char[257];
    Arrays.fill(tooLongValueChars, 'a');
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new TagCreateRequest(
                    "tag_test", null, null, new String[] {new String(tooLongValueChars)})
                .validate());
  }
}
