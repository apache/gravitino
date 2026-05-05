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

package org.apache.gravitino.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpGroupDTO {

  @Test
  public void testIdpGroupDTOSerDe() throws JsonProcessingException {
    IdpGroupDTO groupDTO =
        IdpGroupDTO.builder()
            .withName("test_group")
            .withUsers(Arrays.asList("user1", "user2"))
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(groupDTO);
    IdpGroupDTO deserialized = JsonUtils.objectMapper().readValue(json, IdpGroupDTO.class);

    Assertions.assertEquals(groupDTO, deserialized);
    Assertions.assertEquals("test_group", deserialized.name());
    Assertions.assertEquals(Arrays.asList("user1", "user2"), deserialized.users());

    // Test with default users
    IdpGroupDTO groupDTO1 = IdpGroupDTO.builder().withName("test_group").build();

    String json1 = JsonUtils.objectMapper().writeValueAsString(groupDTO1);
    IdpGroupDTO deserialized1 = JsonUtils.objectMapper().readValue(json1, IdpGroupDTO.class);

    Assertions.assertEquals(groupDTO1, deserialized1);
    Assertions.assertEquals("test_group", deserialized1.name());
    Assertions.assertTrue(deserialized1.users().isEmpty());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> IdpGroupDTO.builder().withName(" ").build());
  }
}
