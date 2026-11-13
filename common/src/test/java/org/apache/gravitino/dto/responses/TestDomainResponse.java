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
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.dto.domain.DomainDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainResponse {

  private static final String DOMAIN_JSON =
      "{\"name\":\"Finance\",\"comment\":null,\"properties\":{},\"status\":\"draft\","
          + "\"parent\":null,\"backingTag\":null,\"audit\":{\"creator\":\"user\","
          + "\"createTime\":\"2026-10-01T00:00:00Z\",\"lastModifier\":null,\"lastModifiedTime\":null}}";

  @Test
  public void testJsonSerialization() throws IOException {
    DomainDTO domain = JsonUtils.objectMapper().readValue(DOMAIN_JSON, DomainDTO.class);
    DomainResponse response = new DomainResponse(domain);
    response.validate();
    Assertions.assertEquals(0, response.getCode());
    Assertions.assertSame(domain, response.getDomain());
    assertJsonEquals("{\"code\":0,\"domain\":" + DOMAIN_JSON + "}", response);
  }

  @Test
  public void testJsonDeserialization() throws IOException {
    String json = "{\"code\":0,\"domain\":" + DOMAIN_JSON + "}";
    DomainResponse response = JsonUtils.objectMapper().readValue(json, DomainResponse.class);
    response.validate();
    Assertions.assertEquals(0, response.getCode());
    Assertions.assertEquals("Finance", response.getDomain().getName());
    Assertions.assertEquals(Domain.Status.DRAFT, response.getDomain().getStatus());
    Assertions.assertEquals("user", response.getDomain().getAudit().creator());
    Assertions.assertEquals(0, response.getDomain().getRelations().length);
    assertJsonEquals(json, response);
  }

  @Test
  public void testValidate() throws IOException {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new DomainResponse().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new DomainResponse(null).validate());
    for (String json :
        new String[] {
          "{\"code\":0}",
          "{\"code\":0,\"domain\":null}",
          "{\"code\":-1,\"domain\":" + DOMAIN_JSON + "}"
        }) {
      DomainResponse response = JsonUtils.objectMapper().readValue(json, DomainResponse.class);
      Assertions.assertThrows(IllegalArgumentException.class, response::validate);
    }
  }

  private static void assertJsonEquals(String expected, Object response) throws IOException {
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected),
        JsonUtils.objectMapper().readTree(JsonUtils.objectMapper().writeValueAsString(response)));
  }
}
