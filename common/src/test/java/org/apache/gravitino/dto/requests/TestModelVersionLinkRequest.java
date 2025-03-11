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

public class TestModelVersionLinkRequest {

  @Test
  public void testModelVersionLinkRequestSerDe() throws JsonProcessingException {
    Map<String, String> props = ImmutableMap.of("key", "value");
    ModelVersionLinkRequest request =
        new ModelVersionLinkRequest("uri", new String[] {"alias1", "alias2"}, "comment", props);

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    ModelVersionLinkRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, ModelVersionLinkRequest.class);

    Assertions.assertEquals(request, deserRequest);

    // Test with null aliases
    ModelVersionLinkRequest request1 = new ModelVersionLinkRequest("uri", null, "comment", props);

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    ModelVersionLinkRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, ModelVersionLinkRequest.class);

    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertNull(deserRequest1.getAliases());

    // Test with empty aliases
    ModelVersionLinkRequest request2 =
        new ModelVersionLinkRequest("uri", new String[] {}, "comment", props);

    String serJson2 = JsonUtils.objectMapper().writeValueAsString(request2);
    ModelVersionLinkRequest deserRequest2 =
        JsonUtils.objectMapper().readValue(serJson2, ModelVersionLinkRequest.class);

    Assertions.assertEquals(request2, deserRequest2);
    Assertions.assertEquals(0, deserRequest2.getAliases().length);

    // Test with null comment and properties
    ModelVersionLinkRequest request3 =
        new ModelVersionLinkRequest("uri", new String[] {"alias1", "alias2"}, null, null);

    String serJson3 = JsonUtils.objectMapper().writeValueAsString(request3);
    ModelVersionLinkRequest deserRequest3 =
        JsonUtils.objectMapper().readValue(serJson3, ModelVersionLinkRequest.class);

    Assertions.assertEquals(request3, deserRequest3);
    Assertions.assertNull(deserRequest3.getComment());
    Assertions.assertNull(deserRequest3.getProperties());
  }
}
