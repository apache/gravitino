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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import java.io.IOException;
import org.apache.gravitino.dto.domain.DomainBackingTagDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.tag.TagValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainBackingTagSetRequest {

  @Test
  public void testMissingFieldIsRejected() throws IOException {
    for (String json : new String[] {"{}", "{\"backing_tag\":null}"}) {
      DomainBackingTagSetRequest request =
          JsonUtils.objectMapper()
              .copy()
              .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
              .readValue(json, DomainBackingTagSetRequest.class);
      Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    }
  }

  @Test
  public void testExplicitNullRemovesBinding() throws IOException {
    DomainBackingTagSetRequest request =
        JsonUtils.objectMapper()
            .readValue("{\"backingTag\":null}", DomainBackingTagSetRequest.class);
    request.validate();
    Assertions.assertNull(request.getBackingTag());
    DomainBackingTagSetRequest removal = new DomainBackingTagSetRequest(null);
    removal.validate();
    // Production mappers omit null fields globally, but an explicit removal must remain explicit.
    String json =
        JsonUtils.objectMapper()
            .copy()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .writeValueAsString(removal);
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree("{\"backingTag\":null}"),
        JsonUtils.objectMapper().readTree(json));
    JsonUtils.objectMapper().readValue(json, DomainBackingTagSetRequest.class).validate();
  }

  @Test
  public void testSetBinding() throws IOException {
    DomainBackingTagSetRequest request =
        JsonUtils.objectMapper()
            .readValue(
                "{\"backingTag\":{\"name\":\"department\",\"value\":\"finance\"}}",
                DomainBackingTagSetRequest.class);
    request.validate();
    Assertions.assertEquals(
        TagValue.of("department", "finance"), request.getBackingTag().tagValue());
    DomainBackingTagSetRequest direct =
        new DomainBackingTagSetRequest(new DomainBackingTagDTO("department", null));
    direct.validate();
    Assertions.assertEquals(TagValue.noValue("department"), direct.getBackingTag().tagValue());
  }
}
