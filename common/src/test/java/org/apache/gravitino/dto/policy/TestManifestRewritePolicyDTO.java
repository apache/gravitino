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

import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestManifestRewritePolicyDTO {
  @Test
  void testPolicyAndCreateRequestRoundTrip() throws Exception {
    IcebergRewriteManifestsContent content =
        PolicyContents.icebergRewriteManifests(900L, 200L, 4096L, 3, false);
    String persisted = JsonUtils.anyFieldMapper().writeValueAsString(content);
    Assertions.assertEquals(
        content,
        JsonUtils.anyFieldMapper().readValue(persisted, IcebergRewriteManifestsContent.class));
    PolicyContentDTO dto = DTOConverters.toDTO(content);
    Assertions.assertEquals(content, DTOConverters.fromDTO(dto));
    String json =
        "{\"name\":\"rewrite\",\"policyType\":\"system_iceberg_rewrite_manifests\","
            + "\"enabled\":true,\"content\":"
            + JsonUtils.objectMapper().writeValueAsString(dto)
            + "}";
    PolicyDTO policy = JsonUtils.objectMapper().readValue(json, PolicyDTO.class);
    Assertions.assertEquals(content, DTOConverters.fromDTO(policy.content()));
    PolicyCreateRequest request =
        JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class);
    request.validate();
    Assertions.assertEquals(content, DTOConverters.fromDTO(request.getPolicyContent()));
  }

  @Test
  void testUpdateAndOmittedDefaults() throws Exception {
    String json =
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_rewrite_manifests\",\"newContent\":{}}";
    PolicyUpdateRequest.UpdatePolicyContentRequest request =
        JsonUtils.objectMapper()
            .readValue(json, PolicyUpdateRequest.UpdatePolicyContentRequest.class);
    request.validate();
    PolicyContentDTO.IcebergRewriteManifestsContentDTO dto =
        JsonUtils.objectMapper()
            .readValue("{}", PolicyContentDTO.IcebergRewriteManifestsContentDTO.class);
    Assertions.assertEquals(PolicyContents.icebergRewriteManifests(), DTOConverters.fromDTO(dto));
    dto.validate();
    PolicyContentDTO.IcebergRewriteManifestsContentDTO invalid =
        JsonUtils.objectMapper()
            .readValue(
                "{\"spec_id\":-1}", PolicyContentDTO.IcebergRewriteManifestsContentDTO.class);
    Assertions.assertThrows(IllegalArgumentException.class, invalid::validate);
  }
}
