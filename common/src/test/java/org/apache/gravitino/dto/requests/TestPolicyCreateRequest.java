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
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyCreateRequest {

  @Test
  public void testPolicyCreateRequestSerDe() throws JsonProcessingException {
    PolicyContentDTO content =
        PolicyContentDTO.CustomContentDTO.builder()
            .withCustomRules(ImmutableMap.of("rule1", "value1"))
            .withProperties(null)
            .build();
    PolicyCreateRequest request =
        new PolicyCreateRequest(
            "policy_test",
            "test_type",
            "policy comment",
            true,
            true,
            true,
            Policy.SUPPORTS_ALL_OBJECT_TYPES,
            content);
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    PolicyCreateRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, PolicyCreateRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("policy_test", deserRequest.getName());
    Assertions.assertEquals("policy comment", deserRequest.getComment());
    Assertions.assertEquals("test_type", deserRequest.getPolicyType());
    Assertions.assertTrue(deserRequest.getEnabled());
    Assertions.assertTrue(deserRequest.getExclusive());
    Assertions.assertTrue(deserRequest.getInheritable());
    Assertions.assertEquals(
        Policy.SUPPORTS_ALL_OBJECT_TYPES, deserRequest.getSupportedObjectTypes());
    Assertions.assertEquals(content, deserRequest.getPolicyContent());

    // test data compaction content
    content = PolicyContentDTO.DataCompactionContentDTO.builder().withTargetSizeBytes(123).build();
    request =
        new PolicyCreateRequest(
            "policy_test",
            "system_data_compaction",
            "policy comment",
            true,
            true,
            true,
            Policy.SUPPORTS_ALL_OBJECT_TYPES,
            content);
    serJson = JsonUtils.objectMapper().writeValueAsString(request);
    deserRequest = JsonUtils.objectMapper().readValue(serJson, PolicyCreateRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("policy_test", deserRequest.getName());
    Assertions.assertEquals("policy comment", deserRequest.getComment());
    Assertions.assertEquals("system_data_compaction", deserRequest.getPolicyType());
    Assertions.assertTrue(deserRequest.getEnabled());
    Assertions.assertTrue(deserRequest.getExclusive());
    Assertions.assertTrue(deserRequest.getInheritable());
    Assertions.assertEquals(
        Policy.SUPPORTS_ALL_OBJECT_TYPES, deserRequest.getSupportedObjectTypes());
    Assertions.assertEquals(content, deserRequest.getPolicyContent());
  }
}
