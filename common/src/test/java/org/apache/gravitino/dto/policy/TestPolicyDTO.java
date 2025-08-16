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
package org.apache.gravitino.dto.policy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyDTO {

  @Test
  public void testPolicySerDe() throws JsonProcessingException {
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();
    PolicyContentDTO.CustomContentDTO customContent =
        PolicyContentDTO.CustomContentDTO.builder()
            .withCustomRules(ImmutableMap.of("key1", "value1"))
            .withSupportedObjectTypes(
                ImmutableSet.of(MetadataObject.Type.CATALOG, MetadataObject.Type.TABLE))
            .withProperties(ImmutableMap.of("prop1", "value1"))
            .build();

    PolicyDTO policyDTO =
        PolicyDTO.builder()
            .withName("policy_test")
            .withComment("policy comment")
            .withPolicyType("my_compaction")
            .withEnabled(true)
            .withContent(customContent)
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(policyDTO);
    PolicyDTO deserPolicyDTO = JsonUtils.objectMapper().readValue(serJson, PolicyDTO.class);
    Assertions.assertEquals(policyDTO, deserPolicyDTO);

    Assertions.assertEquals("policy_test", deserPolicyDTO.name());
    Assertions.assertEquals("policy comment", deserPolicyDTO.comment());
    Assertions.assertEquals("my_compaction", deserPolicyDTO.policyType());
    Assertions.assertTrue(deserPolicyDTO.enabled());
    Assertions.assertEquals(customContent, deserPolicyDTO.content());
    Assertions.assertEquals(audit, deserPolicyDTO.auditInfo());

    // Test policy with inherited
    PolicyDTO policyDTO2 =
        PolicyDTO.builder()
            .withName("policy_test")
            .withComment("policy comment")
            .withPolicyType("my_compaction")
            .withContent(customContent)
            .withAudit(audit)
            .withInherited(Optional.empty())
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(policyDTO2);
    PolicyDTO deserPolicyDTO2 = JsonUtils.objectMapper().readValue(serJson, PolicyDTO.class);
    Assertions.assertEquals(policyDTO2, deserPolicyDTO2);
    Assertions.assertEquals(Optional.empty(), deserPolicyDTO2.inherited());

    PolicyDTO policyDTO3 =
        PolicyDTO.builder()
            .withName("policy_test")
            .withComment("policy comment")
            .withPolicyType("my_compaction")
            .withContent(customContent)
            .withAudit(audit)
            .withInherited(Optional.of(false))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(policyDTO3);
    PolicyDTO deserPolicyDTO3 = JsonUtils.objectMapper().readValue(serJson, PolicyDTO.class);
    Assertions.assertEquals(Optional.of(false), deserPolicyDTO3.inherited());

    PolicyDTO policyDTO4 =
        PolicyDTO.builder()
            .withName("policy_test")
            .withComment("policy comment")
            .withPolicyType("my_compaction")
            .withContent(customContent)
            .withAudit(audit)
            .withInherited(Optional.of(true))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(policyDTO4);
    PolicyDTO deserPolicyDTO4 = JsonUtils.objectMapper().readValue(serJson, PolicyDTO.class);
    Assertions.assertEquals(Optional.of(true), deserPolicyDTO4.inherited());
  }
}
