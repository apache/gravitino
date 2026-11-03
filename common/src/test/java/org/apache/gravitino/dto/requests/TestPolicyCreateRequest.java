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
import com.google.common.collect.ImmutableSet;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyCreateRequest {

  @Test
  public void testPolicyCreateRequestSerDe() throws JsonProcessingException {
    PolicyContentDTO content =
        PolicyContentDTO.CustomContentDTO.builder()
            .withCustomRules(ImmutableMap.of("rule1", "value1"))
            .withSupportedObjectTypes(
                ImmutableSet.of(MetadataObject.Type.FILESET, MetadataObject.Type.TABLE))
            .withProperties(null)
            .build();
    PolicyCreateRequest request =
        new PolicyCreateRequest("policy_test", "test_type", "policy comment", true, content);
    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    PolicyCreateRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, PolicyCreateRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("policy_test", deserRequest.getName());
    Assertions.assertEquals("policy comment", deserRequest.getComment());
    Assertions.assertEquals("test_type", deserRequest.getPolicyType());
    Assertions.assertTrue(deserRequest.getEnabled());
    Assertions.assertEquals(content, deserRequest.getPolicyContent());

    // test null supported object types in content
    PolicyContentDTO contentWithNullTypes =
        PolicyContentDTO.CustomContentDTO.builder()
            .withCustomRules(ImmutableMap.of("rule2", "value2"))
            .withSupportedObjectTypes(null)
            .withProperties(null)
            .build();
    request =
        new PolicyCreateRequest(
            "policy_test_null_types", "custom", "policy comment", true, contentWithNullTypes);
    serJson = JsonUtils.objectMapper().writeValueAsString(request);
    deserRequest = JsonUtils.objectMapper().readValue(serJson, PolicyCreateRequest.class);
    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals("policy_test_null_types", deserRequest.getName());
    Assertions.assertEquals("policy comment", deserRequest.getComment());
    Assertions.assertEquals("custom", deserRequest.getPolicyType());
    Assertions.assertTrue(deserRequest.getEnabled());
    Assertions.assertEquals(contentWithNullTypes, deserRequest.getPolicyContent());

    Exception e = Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertEquals("supportedObjectTypes cannot be empty", e.getMessage());
  }

  @Test
  public void testDeserializeRejectsUnknownOrMisCasedPolicyType() {
    assertPolicyTypeNamedOnDeserialize(
        "{\"name\":\"p_bogus\",\"policyType\":\"system_iceberg_bogus\",\"enabled\":true,\"content\":{}}",
        "Unknown built-in policy type: system_iceberg_bogus");
    assertPolicyTypeNamedOnDeserialize(
        "{\"name\":\"p_upper\",\"policyType\":\"SYSTEM_ICEBERG_COMPACTION\",\"enabled\":true,\"content\":{}}",
        "Policy type must be 'system_iceberg_compaction', but got: SYSTEM_ICEBERG_COMPACTION");
  }

  @Test
  public void testDeserializeRejectsMalformedEnabled() {
    String content = "\"content\":{\"customRules\":{},\"supportedObjectTypes\":[\"table\"]}}";
    for (String enabled : new String[] {"\"typo\"", "{}", "[]"}) {
      String json =
          "{\"name\":\"p\",\"policyType\":\"custom\",\"enabled\":" + enabled + "," + content;
      Assertions.assertThrows(
          JsonProcessingException.class,
          () -> JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class),
          enabled);
    }
  }

  @Test
  public void testDeserializeRejectsUnknownProperty() {
    String json =
        "{\"name\":\"p\",\"policyType\":\"custom\",\"enabld\":false,"
            + "\"content\":{\"customRules\":{},\"supportedObjectTypes\":[\"table\"]}}";
    JsonProcessingException thrown =
        Assertions.assertThrows(
            JsonProcessingException.class,
            () -> JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class));
    Assertions.assertTrue(thrown.getMessage().contains("enabld"), thrown.getMessage());
  }

  @Test
  public void testDeserializeEnabledFalse() throws JsonProcessingException {
    String json =
        "{\"name\":\"p\",\"policyType\":\"custom\",\"enabled\":false,"
            + "\"content\":{\"customRules\":{},\"supportedObjectTypes\":[\"table\"]}}";
    PolicyCreateRequest request =
        JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class);
    Assertions.assertFalse(request.getEnabled());
  }

  @Test
  public void testDeserializeRejectsMisCasedTypeWithBuiltInContent() {
    // Non-empty built-in content must not become "Malformed json" via CustomContentDTO.
    assertPolicyTypeNamedOnDeserialize(
        "{\"name\":\"p_upper\",\"policyType\":\"SYSTEM_ICEBERG_COMPACTION\",\"enabled\":true,"
            + "\"content\":{\"maxPartitionNum\":5}}",
        "Policy type must be 'system_iceberg_compaction', but got: SYSTEM_ICEBERG_COMPACTION");
    assertPolicyTypeNamedOnDeserialize(
        "{\"name\":\"p_bogus\",\"policyType\":\"system_iceberg_bogus\",\"enabled\":true,"
            + "\"content\":{\"maxPartitionNum\":5}}",
        "Unknown built-in policy type: system_iceberg_bogus");
  }

  private static void assertPolicyTypeNamedOnDeserialize(String json, String expectedMessage) {
    Exception thrown =
        Assertions.assertThrows(
            Exception.class,
            () -> JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class));
    Throwable cause = thrown;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    Assertions.assertInstanceOf(IllegalArgumentException.class, cause);
    Assertions.assertEquals(expectedMessage, cause.getMessage());
  }
}
