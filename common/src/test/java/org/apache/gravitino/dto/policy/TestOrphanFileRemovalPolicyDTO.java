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

import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestOrphanFileRemovalPolicyDTO {
  @Test
  void createRequestDefaultsAndValidation() throws Exception {
    String json =
        "{\"name\":\"cleanup\",\"policyType\":\"system_iceberg_orphan_file_removal\",\"enabled\":true,\"content\":{}}";
    PolicyCreateRequest request =
        JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class);
    request.validate();
    Assertions.assertEquals(
        PolicyContents.icebergOrphanFileRemoval(),
        DTOConverters.fromDTO(request.getPolicyContent()));
    String invalid = json.replace("\"content\":{}", "\"content\":{\"olderThanDays\":0}");
    PolicyCreateRequest bad =
        JsonUtils.objectMapper().readValue(invalid, PolicyCreateRequest.class);
    Assertions.assertThrows(IllegalArgumentException.class, bad::validate);
  }

  @Test
  void updateContentRoundTrip() throws Exception {
    String json =
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_orphan_file_removal\",\"newContent\":{\"olderThanDays\":7,\"dryRun\":true}}";
    PolicyUpdateRequest request =
        JsonUtils.objectMapper().readValue(json, PolicyUpdateRequest.class);
    request.validate();
    PolicyUpdateRequest.UpdatePolicyContentRequest update =
        (PolicyUpdateRequest.UpdatePolicyContentRequest) request;
    Assertions.assertEquals(
        PolicyContents.icebergOrphanFileRemoval(7, null, true),
        DTOConverters.fromDTO(update.getNewContent()));
    Assertions.assertEquals(
        request,
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(request), PolicyUpdateRequest.class));
  }

  @Test
  void responseRoundTripRetainsTypedContent() throws Exception {
    PolicyContent domain =
        PolicyContents.icebergOrphanFileRemoval(7, "s3://bucket/table/data", true);
    PolicyDTO dto =
        PolicyDTO.builder()
            .withName("cleanup")
            .withPolicyType("system_iceberg_orphan_file_removal")
            .withEnabled(true)
            .withContent(DTOConverters.toDTO(domain))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
            .build();
    PolicyDTO restored =
        JsonUtils.objectMapper()
            .readValue(JsonUtils.objectMapper().writeValueAsString(dto), PolicyDTO.class);
    Assertions.assertEquals(dto, restored);
    Assertions.assertEquals(domain, DTOConverters.fromDTO(restored.content()));
  }

  @Test
  void createAndUpdateValidateRetentionUpperBoundary() throws Exception {
    long maximum = IcebergOrphanFileRemovalContent.MAX_OLDER_THAN_DAYS;
    for (long days : new long[] {maximum, maximum + 1, Long.MAX_VALUE}) {
      String content = "{\"olderThanDays\":" + days + "}";
      PolicyCreateRequest create =
          JsonUtils.objectMapper()
              .readValue(
                  "{\"name\":\"cleanup\",\"policyType\":\"system_iceberg_orphan_file_removal\",\"content\":"
                      + content
                      + "}",
                  PolicyCreateRequest.class);
      PolicyUpdateRequest update =
          JsonUtils.objectMapper()
              .readValue(
                  "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_orphan_file_removal\",\"newContent\":"
                      + content
                      + "}",
                  PolicyUpdateRequest.class);
      if (days == maximum) {
        Assertions.assertDoesNotThrow(create::validate);
        Assertions.assertDoesNotThrow(update::validate);
      } else {
        Assertions.assertTrue(
            Assertions.assertThrows(IllegalArgumentException.class, create::validate)
                .getMessage()
                .contains("olderThanDays"));
        Assertions.assertTrue(
            Assertions.assertThrows(IllegalArgumentException.class, update::validate)
                .getMessage()
                .contains("olderThanDays"));
      }
    }
  }
}
