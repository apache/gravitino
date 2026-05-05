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

package org.apache.gravitino.auth.local.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Test;

public class TestIdpUserDTO {

  @Test
  public void testSerDe() throws JsonProcessingException {
    IdpUserDTO userDTO =
        IdpUserDTO.builder()
            .withName("test_user")
            .withGroups(Arrays.asList("group1", "group2"))
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(userDTO);
    IdpUserDTO deserialized = JsonUtils.objectMapper().readValue(json, IdpUserDTO.class);

    assertEquals("test_user", deserialized.name());
    assertEquals(Arrays.asList("group1", "group2"), deserialized.groups());
  }

  @Test
  public void testBuilderDefaults() {
    IdpUserDTO userDTO = IdpUserDTO.builder().withName("test_user").build();

    assertEquals("test_user", userDTO.name());
    assertEquals(Collections.emptyList(), userDTO.groups());
  }

  @Test
  public void testBuilderRequiresName() {
    assertThrows(IllegalArgumentException.class, () -> IdpUserDTO.builder().withName(" ").build());
  }
}
