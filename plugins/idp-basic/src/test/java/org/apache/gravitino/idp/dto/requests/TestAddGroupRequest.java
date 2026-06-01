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

package org.apache.gravitino.idp.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAddGroupRequest {

  @Test
  public void testAddGroupRequestSerDe() throws JsonProcessingException {
    AddGroupRequest request = new AddGroupRequest("test_group");

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    AddGroupRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, AddGroupRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("test_group", deserRequest.getGroup());

    // Test with null group
    AddGroupRequest request1 = new AddGroupRequest();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(request1);
    AddGroupRequest deserRequest1 =
        JsonUtils.objectMapper().readValue(serJson1, AddGroupRequest.class);

    Assertions.assertEquals(request1, deserRequest1);
    Assertions.assertNull(deserRequest1.getGroup());
  }

  @Test
  public void testAddGroupRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new AddGroupRequest("test_group").validate());
    Assertions.assertThrows(IllegalArgumentException.class, () -> new AddGroupRequest().validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new AddGroupRequest(" ").validate());
  }
}
