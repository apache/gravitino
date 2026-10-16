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

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRewriteManifestsAndSnapshotExpirationPolicyDTO {

  @Test
  void rewriteManifestsCreateAndUpdateRequestTypedContent() throws Exception {
    assertCreateAndUpdateRoundTrip(
        "system_iceberg_rewrite_manifests", PolicyContents.icebergRewriteManifests());
  }

  @Test
  void snapshotExpirationCreateAndUpdateRequestTypedContent() throws Exception {
    assertCreateAndUpdateRoundTrip(
        "system_iceberg_snapshot_expiration", PolicyContents.icebergSnapshotExpiration());
  }

  @Test
  void rewriteManifestsResponseRoundTripWithExecutionPeers() throws Exception {
    assertResponseRoundTripWithPeers(
        "system_iceberg_rewrite_manifests",
        PolicyContents.icebergRewriteManifests(
            "0 30 1 * * ?",
            true,
            900_000L,
            ImmutableMap.of("spark.executor.memory", "2g"),
            ImmutableMap.of("queue", "tms")));
  }

  @Test
  void snapshotExpirationResponseRoundTripWithExecutionPeers() throws Exception {
    assertResponseRoundTripWithPeers(
        "system_iceberg_snapshot_expiration",
        PolicyContents.icebergSnapshotExpiration(
            "0 0 4 * * ?",
            false,
            2_700_000L,
            ImmutableMap.of("spark.driver.memory", "1g"),
            ImmutableMap.of("metalake", "ml")));
  }

  private static void assertCreateAndUpdateRoundTrip(String policyType, PolicyContent expected)
      throws Exception {
    PolicyCreateRequest create =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"p\",\"policyType\":\""
                    + policyType
                    + "\",\"enabled\":true,\"content\":{}}",
                PolicyCreateRequest.class);
    create.validate();
    Assertions.assertEquals(expected, DTOConverters.fromDTO(create.getPolicyContent()));

    PolicyUpdateRequest update =
        JsonUtils.objectMapper()
            .readValue(
                "{\"@type\":\"updateContent\",\"policyType\":\""
                    + policyType
                    + "\",\"newContent\":{}}",
                PolicyUpdateRequest.class);
    update.validate();
    PolicyUpdateRequest.UpdatePolicyContentRequest contentUpdate =
        (PolicyUpdateRequest.UpdatePolicyContentRequest) update;
    Assertions.assertEquals(expected, DTOConverters.fromDTO(contentUpdate.getNewContent()));
  }

  private static void assertResponseRoundTripWithPeers(String policyType, PolicyContent domain)
      throws Exception {
    PolicyDTO dto =
        PolicyDTO.builder()
            .withName("p")
            .withPolicyType(policyType)
            .withEnabled(true)
            .withContent(DTOConverters.toDTO(domain))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
            .build();
    String json = JsonUtils.objectMapper().writeValueAsString(dto);
    Assertions.assertTrue(json.contains("\"timeTrigger\""));
    Assertions.assertFalse(json.contains("\"executionSpec\""));
    PolicyDTO restored = JsonUtils.objectMapper().readValue(json, PolicyDTO.class);
    Assertions.assertEquals(dto, restored);
    Assertions.assertEquals(domain, DTOConverters.fromDTO(restored.content()));
  }
}
