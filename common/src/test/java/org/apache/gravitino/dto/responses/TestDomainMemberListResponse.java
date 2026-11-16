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
import org.apache.gravitino.dto.domain.DomainMemberDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainMemberListResponse {

  private static final String ITEM_JSON =
      "{\"object\":{\"type\":\"table\",\"fullName\":\"cat.schema.table\"},"
          + "\"relations\":[{\"relation\":\"via_subdomain\",\"source\":{\"type\":\"domain\",\"fullName\":\"Finance_US\"}}]}";

  @Test
  public void testJsonSerialization() throws IOException {
    DomainMemberDTO item = JsonUtils.objectMapper().readValue(ITEM_JSON, DomainMemberDTO.class);
    DomainMemberListResponse response = new DomainMemberListResponse(new DomainMemberDTO[] {item});
    response.validate();
    Assertions.assertEquals(0, response.getCode());
    assertJsonEquals("{\"code\":0,\"members\":[" + ITEM_JSON + "]}", response);
  }

  @Test
  public void testJsonDeserialization() throws IOException {
    String json = "{\"code\":0,\"members\":[" + ITEM_JSON + "]}";
    DomainMemberListResponse response =
        JsonUtils.objectMapper().readValue(json, DomainMemberListResponse.class);
    response.validate();
    Assertions.assertEquals(0, response.getCode());
    Assertions.assertEquals(1, response.getMembers().length);
    Assertions.assertEquals("cat.schema.table", response.getMembers()[0].getObject().fullName());
    Assertions.assertEquals(
        "Finance_US", response.getMembers()[0].getRelations()[0].getSource().name());
    Assertions.assertNull(response.getMembers()[0].getRelations()[0].getSource().parent());
    assertJsonEquals(json, response);
  }

  @Test
  public void testEmptyList() throws IOException {
    String json = "{\"code\":0,\"members\":[]}";
    DomainMemberListResponse response =
        JsonUtils.objectMapper().readValue(json, DomainMemberListResponse.class);
    response.validate();
    Assertions.assertEquals(0, response.getMembers().length);
    assertJsonEquals(json, response);
    assertJsonEquals(json, new DomainMemberListResponse(new DomainMemberDTO[0]));
  }

  @Test
  public void testValidate() throws IOException {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new DomainMemberListResponse().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new DomainMemberListResponse(new DomainMemberDTO[] {null}).validate());
    for (String json :
        new String[] {
          "{\"code\":0}",
          "{\"code\":0,\"members\":null}",
          "{\"code\":0,\"members\":[null]}",
          "{\"code\":-1,\"members\":[]}"
        }) {
      DomainMemberListResponse response =
          JsonUtils.objectMapper().readValue(json, DomainMemberListResponse.class);
      Assertions.assertThrows(IllegalArgumentException.class, response::validate);
    }
  }

  @Test
  public void testDefensiveCopies() throws IOException {
    DomainMemberDTO item = JsonUtils.objectMapper().readValue(ITEM_JSON, DomainMemberDTO.class);
    DomainMemberDTO[] input = {item};
    DomainMemberListResponse response = new DomainMemberListResponse(input);
    input[0] = null;
    Assertions.assertSame(item, response.getMembers()[0]);
    response.getMembers()[0] = null;
    Assertions.assertSame(item, response.getMembers()[0]);
    response.validate();
  }

  private static void assertJsonEquals(String expected, Object response) throws IOException {
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected),
        JsonUtils.objectMapper().readTree(JsonUtils.objectMapper().writeValueAsString(response)));
  }
}
