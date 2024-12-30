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
import java.util.Map;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelRegisterRequest {

  @Test
  public void testModelRegisterRequestSerDe() throws JsonProcessingException {
    Map<String, String> props = ImmutableMap.of("key", "value");
    ModelRegisterRequest req = new ModelRegisterRequest("model", "comment", props);

    String serJson = JsonUtils.objectMapper().writeValueAsString(req);
    ModelRegisterRequest deserReq =
        JsonUtils.objectMapper().readValue(serJson, ModelRegisterRequest.class);
    Assertions.assertEquals(req, deserReq);

    // Test with null comment and properties
    ModelRegisterRequest req1 = new ModelRegisterRequest("model", null, null);
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(req1);
    ModelRegisterRequest deserReq1 =
        JsonUtils.objectMapper().readValue(serJson1, ModelRegisterRequest.class);
    Assertions.assertEquals(req1, deserReq1);
  }
}
