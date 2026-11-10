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
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestPrivilegeOverrideRequest {

  @ParameterizedTest
  @ValueSource(strings = {"{}", "{\"overrides\":null}", "{\"overrides\":[null]}"})
  void testRejectsMissingOrNullOverrides(String json) throws JsonProcessingException {
    PrivilegeOverrideRequest request =
        JsonUtils.objectMapper().readValue(json, PrivilegeOverrideRequest.class);
    IllegalArgumentException error =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertTrue(error.getMessage().contains("overrides"));
  }

  @Test
  void testAcceptsEmptyOverrides() throws JsonProcessingException {
    PrivilegeOverrideRequest request =
        JsonUtils.objectMapper().readValue("{\"overrides\":[]}", PrivilegeOverrideRequest.class);
    Assertions.assertDoesNotThrow(request::validate);
  }
}
