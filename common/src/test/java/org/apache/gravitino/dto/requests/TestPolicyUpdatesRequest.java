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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestPolicyUpdatesRequest {

  @Test
  public void testRenamePolicyRequestSerDe() throws JsonProcessingException {
    PolicyUpdateRequest.RenamePolicyRequest request =
        new PolicyUpdateRequest.RenamePolicyRequest("policy_test_new");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    PolicyUpdateRequest.RenamePolicyRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, PolicyUpdateRequest.RenamePolicyRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("policy_test_new", deserRequest.getNewName());
  }

  @Test
  public void testUpdatePolicyCommentRequestSerDe() throws JsonProcessingException {
    PolicyUpdateRequest.UpdatePolicyCommentRequest request =
        new PolicyUpdateRequest.UpdatePolicyCommentRequest("policy comment new");
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    PolicyUpdateRequest.UpdatePolicyCommentRequest deserRequest =
        JsonUtils.objectMapper()
            .readValue(serJson, PolicyUpdateRequest.UpdatePolicyCommentRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("policy comment new", deserRequest.getNewComment());
  }

  @Test
  public void testUpdatePolicyContentRequestSerDe() throws JsonProcessingException {
    PolicyContentDTO contentDTO =
        PolicyContentDTO.CustomContentDTO.builder()
            .withCustomRules(ImmutableMap.of("rule1", "value1"))
            .withSupportedObjectTypes(ImmutableSet.of(MetadataObject.Type.TABLE))
            .withProperties(ImmutableMap.of("key", "value"))
            .build();
    PolicyUpdateRequest.UpdatePolicyContentRequest request =
        new PolicyUpdateRequest.UpdatePolicyContentRequest("custom", contentDTO);
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    PolicyUpdateRequest.UpdatePolicyContentRequest deserRequest =
        JsonUtils.objectMapper()
            .readValue(serJson, PolicyUpdateRequest.UpdatePolicyContentRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("custom", deserRequest.getPolicyType());
    Assertions.assertEquals(contentDTO, deserRequest.getNewContent());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "system_iceberg_compaction",
        "SYSTEM_ICEBERG_COMPACTION",
        "System_Iceberg_Compaction"
      })
  public void testUpdatePolicyContentAcceptsKnownPolicyTypeCaseInsensitively(String policyType)
      throws JsonProcessingException {
    String json =
        "{\"@type\":\"updateContent\",\"policyType\":\""
            + policyType
            + "\",\"newContent\":{\"maxPartitionNum\":5}}";
    PolicyUpdateRequest.UpdatePolicyContentRequest request =
        JsonUtils.objectMapper()
            .readValue(json, PolicyUpdateRequest.UpdatePolicyContentRequest.class);
    Assertions.assertEquals(policyType, request.getPolicyType());
    Assertions.assertInstanceOf(
        PolicyContentDTO.IcebergCompactionContentDTO.class, request.getNewContent());
    request.validate();
    Assertions.assertTrue(
        request
            .policyChange()
            .toString()
            .contains(Policy.BuiltInType.ICEBERG_COMPACTION.policyType()),
        request.policyChange().toString());
  }

  @Test
  public void testUpdatePolicyContentDeserializeRejectsUnknownPolicyType()
      throws JsonProcessingException {
    assertPolicyTypeNamedOnDeserialize(
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_bogus\","
            + "\"newContent\":{}}",
        "Unknown built-in policy type: system_iceberg_bogus");
    assertPolicyTypeNamedOnDeserialize(
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_bogus\","
            + "\"newContent\":{\"maxPartitionNum\":5}}",
        "Unknown built-in policy type: system_iceberg_bogus");
  }

  private static void assertPolicyTypeNamedOnDeserialize(String json, String expectedMessage)
      throws JsonProcessingException {
    PolicyUpdateRequest.UpdatePolicyContentRequest request =
        JsonUtils.objectMapper()
            .readValue(json, PolicyUpdateRequest.UpdatePolicyContentRequest.class);
    Assertions.assertNull(request.getNewContent());
    IllegalArgumentException thrown =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertEquals(expectedMessage, thrown.getMessage());
  }

  @Test
  public void testPolicyUpdatesRequestSerDe() throws JsonProcessingException {
    PolicyUpdateRequest request = new PolicyUpdateRequest.RenamePolicyRequest("policy_test_new");
    PolicyUpdateRequest request1 =
        new PolicyUpdateRequest.UpdatePolicyCommentRequest("policy comment new");
    PolicyUpdateRequest request2 =
        new PolicyUpdateRequest.UpdatePolicyContentRequest(
            "custom",
            PolicyContentDTO.CustomContentDTO.builder()
                .withCustomRules(ImmutableMap.of("rule1", "value1"))
                .withSupportedObjectTypes(ImmutableSet.of(MetadataObject.Type.TABLE))
                .withProperties(ImmutableMap.of("key", "value"))
                .build());

    List<PolicyUpdateRequest> updates = ImmutableList.of(request, request1, request2);
    PolicyUpdatesRequest policyUpdatesRequest = new PolicyUpdatesRequest(updates);
    String serJson = JsonUtils.objectMapper().writeValueAsString(policyUpdatesRequest);
    PolicyUpdatesRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, PolicyUpdatesRequest.class);
    Assertions.assertEquals(policyUpdatesRequest, deserRequest);
    Assertions.assertEquals(updates, deserRequest.getUpdates());
  }
}
