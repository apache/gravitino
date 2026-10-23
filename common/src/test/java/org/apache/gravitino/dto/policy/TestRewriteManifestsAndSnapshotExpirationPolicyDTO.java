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
}
