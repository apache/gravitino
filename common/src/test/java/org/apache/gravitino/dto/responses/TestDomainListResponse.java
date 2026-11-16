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

package org.apache.gravitino.dto.responses;

import java.io.IOException;
import org.apache.gravitino.dto.domain.DomainDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainListResponse {

  private static final String ITEM_JSON =
      "{\"name\":\"Finance\",\"comment\":null,\"properties\":{},\"status\":\"draft\","
          + "\"parent\":null,\"backingTag\":null,\"audit\":{\"creator\":\"user\","
          + "\"createTime\":\"2026-10-01T00:00:00Z\",\"lastModifier\":null,\"lastModifiedTime\":null}}";

  @Test
  public void testJsonSerialization() throws IOException {
    DomainDTO item = JsonUtils.objectMapper().readValue(ITEM_JSON, DomainDTO.class);
    DomainListResponse response = new DomainListResponse(new DomainDTO[] {item});
    response.validate();
    Assertions.assertEquals(0, response.getCode());
    assertJsonEquals("{\"code\":0,\"domains\":[" + ITEM_JSON + "]}", response);
  }

  @Test
  public void testJsonDeserialization() throws IOException {
    String json = "{\"code\":0,\"domains\":[" + ITEM_JSON + "]}";
    DomainListResponse response =
        JsonUtils.objectMapper().readValue(json, DomainListResponse.class);
    response.validate();
    Assertions.assertEquals(0, response.getCode());
    Assertions.assertEquals(1, response.getDomains().length);
    Assertions.assertEquals("Finance", response.getDomains()[0].getName());
    Assertions.assertEquals("user", response.getDomains()[0].getAudit().creator());
    assertJsonEquals(json, response);
  }

  @Test
  public void testEmptyList() throws IOException {
    String json = "{\"code\":0,\"domains\":[]}";
    DomainListResponse response =
        JsonUtils.objectMapper().readValue(json, DomainListResponse.class);
    response.validate();
    Assertions.assertEquals(0, response.getDomains().length);
    assertJsonEquals(json, response);
    assertJsonEquals(json, new DomainListResponse(new DomainDTO[0]));
  }

  @Test
  public void testValidate() throws IOException {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new DomainListResponse().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new DomainListResponse(new DomainDTO[] {null}).validate());
    for (String json :
        new String[] {
          "{\"code\":0}",
          "{\"code\":0,\"domains\":null}",
          "{\"code\":0,\"domains\":[null]}",
          "{\"code\":-1,\"domains\":[]}"
        }) {
      DomainListResponse response =
          JsonUtils.objectMapper().readValue(json, DomainListResponse.class);
      Assertions.assertThrows(IllegalArgumentException.class, response::validate);
    }
  }

  @Test
  public void testDefensiveCopies() throws IOException {
    DomainDTO item = JsonUtils.objectMapper().readValue(ITEM_JSON, DomainDTO.class);
    DomainDTO[] input = {item};
    DomainListResponse response = new DomainListResponse(input);
    input[0] = null;
    Assertions.assertSame(item, response.getDomains()[0]);
    response.getDomains()[0] = null;
    Assertions.assertSame(item, response.getDomains()[0]);
    response.validate();
  }

  private static void assertJsonEquals(String expected, Object response) throws IOException {
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected),
        JsonUtils.objectMapper().readTree(JsonUtils.objectMapper().writeValueAsString(response)));
  }
}
