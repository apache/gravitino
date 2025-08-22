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
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDropResponse {

  @Test
  public void testDropResponseSerDe() throws JsonProcessingException {
    DropResponse resp = new DropResponse(true);
    String serJson = JsonUtils.objectMapper().writeValueAsString(resp);
    DropResponse deserResp = JsonUtils.objectMapper().readValue(serJson, DropResponse.class);
    Assertions.assertTrue(deserResp.dropped());
    Assertions.assertNull(deserResp.deleted());

    resp = new DropResponse(true, true);
    serJson = JsonUtils.objectMapper().writeValueAsString(resp);
    deserResp = JsonUtils.objectMapper().readValue(serJson, DropResponse.class);
    Assertions.assertTrue(deserResp.dropped());
    Assertions.assertTrue(deserResp.deleted() != null && deserResp.deleted());

    resp = new DropResponse(false, false);
    serJson = JsonUtils.objectMapper().writeValueAsString(resp);
    deserResp = JsonUtils.objectMapper().readValue(serJson, DropResponse.class);
    Assertions.assertFalse(deserResp.dropped());
    Assertions.assertTrue(deserResp.deleted() != null && !deserResp.deleted());
  }

  @Test
  public void deserializeFromString() throws JsonProcessingException {
    String json = "{\"dropped\":true,\"deleted\":null}";
    DropResponse response = JsonUtils.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertTrue(response.dropped());
    Assertions.assertNull(response.deleted());

    json = "{\"dropped\":true,\"deleted\":true}";
    response = JsonUtils.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertTrue(response.dropped());
    Assertions.assertTrue(response.deleted() != null && response.deleted());

    json = "{\"dropped\":false,\"deleted\":false}";
    response = JsonUtils.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertFalse(response.dropped());
    Assertions.assertTrue(response.deleted() != null && !response.deleted());

    json = "{\"dropped\":false}";
    response = JsonUtils.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertFalse(response.dropped());
    Assertions.assertNull(response.deleted());

    json = "{\"dropped\":true}";
    response = JsonUtils.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertTrue(response.dropped());
    Assertions.assertNull(response.deleted());

    json = "{\"deleted\":true}";
    response = JsonUtils.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertTrue(response.dropped());
    Assertions.assertTrue(response.deleted() != null && response.deleted());

    json = "{\"deleted\":false}";
    response = JsonUtils.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertFalse(response.dropped());
    Assertions.assertTrue(response.deleted() != null && !response.deleted());
  }
}
