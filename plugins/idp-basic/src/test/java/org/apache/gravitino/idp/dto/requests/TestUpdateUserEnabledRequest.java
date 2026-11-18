/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

public class TestUpdateUserEnabledRequest {

  @Test
  public void testUpdateUserEnabledRequestSerDe() throws JsonProcessingException {
    UpdateUserEnabledRequest request = new UpdateUserEnabledRequest(false);

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    UpdateUserEnabledRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, UpdateUserEnabledRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertFalse(deserRequest.getEnabled());
    Assertions.assertTrue(serJson.contains("\"enabled\""));
  }

  @Test
  public void testUpdateUserEnabledRequestValidate() {
    Assertions.assertDoesNotThrow(() -> new UpdateUserEnabledRequest(true).validate());
    Assertions.assertDoesNotThrow(() -> new UpdateUserEnabledRequest(false).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new UpdateUserEnabledRequest().validate());
  }
}
