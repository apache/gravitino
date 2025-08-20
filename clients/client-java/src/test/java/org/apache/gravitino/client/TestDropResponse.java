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
package org.apache.gravitino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.dto.responses.DeleteResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDropResponse {

  @Test
  public void testSupportDropResponseSerDe() throws JsonProcessingException {
    // test new server with new client
    String json = "{\"dropped\":true,\"deleted\":null}";
    DropResponse resp = ObjectMapperProvider.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertTrue(resp.dropped());

    json = "{\"dropped\":true,\"deleted\":true}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertTrue(resp.dropped());

    json = "{\"dropped\":false,\"deleted\":false}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertFalse(resp.dropped());

    json = "{\"dropped\":false}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertFalse(resp.dropped());

    json = "{\"dropped\":true}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertTrue(resp.dropped());

    // test old server with new client
    json = "{\"deleted\":true}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertTrue(resp.dropped());

    json = "{\"deleted\":false}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DropResponse.class);
    Assertions.assertFalse(resp.dropped());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSupportDeleteResponseSerDe() throws JsonProcessingException {
    // New server with old client
    String json = "{\"dropped\":true,\"deleted\":true}";
    DeleteResponse resp = ObjectMapperProvider.objectMapper().readValue(json, DeleteResponse.class);
    Assertions.assertTrue(resp.deleted());

    json = "{\"dropped\":false,\"deleted\":false}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DeleteResponse.class);
    Assertions.assertFalse(resp.deleted());

    // Old server with old client
    json = "{\"deleted\":true}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DeleteResponse.class);
    Assertions.assertTrue(resp.deleted());

    json = "{\"deleted\":false}";
    resp = ObjectMapperProvider.objectMapper().readValue(json, DeleteResponse.class);
    Assertions.assertFalse(resp.deleted());
  }
}
