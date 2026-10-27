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
import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.IcebergSnapshotExpirationContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestSnapshotExpirationPolicyDTO {

  @Test
  void createRequestDefaultsAndValidation() throws Exception {
    String json =
        "{\"name\":\"expire\",\"policyType\":\"system_iceberg_snapshot_expiration\",\"enabled\":true,\"content\":{}}";
    PolicyCreateRequest request =
        JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class);
    request.validate();
    Assertions.assertEquals(
        PolicyContents.icebergSnapshotExpiration(),
        DTOConverters.fromDTO(request.getPolicyContent()));
    String invalid = json.replace("\"content\":{}", "\"content\":{\"olderThanDays\":0}");
    PolicyCreateRequest bad =
        JsonUtils.objectMapper().readValue(invalid, PolicyCreateRequest.class);
    Assertions.assertThrows(IllegalArgumentException.class, bad::validate);
  }

  @Test
  void updateContentRoundTrip() throws Exception {
    String json =
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_snapshot_expiration\","
            + "\"newContent\":{\"olderThanDays\":14,\"retainLast\":3,\"streamResults\":true}}";
    PolicyUpdateRequest request =
        JsonUtils.objectMapper().readValue(json, PolicyUpdateRequest.class);
    request.validate();
    PolicyUpdateRequest.UpdatePolicyContentRequest update =
        (PolicyUpdateRequest.UpdatePolicyContentRequest) request;
    Assertions.assertEquals(
        PolicyContents.icebergSnapshotExpiration(14L, 3L, true),
        DTOConverters.fromDTO(update.getNewContent()));
    Assertions.assertEquals(
        request,
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(request), PolicyUpdateRequest.class));
  }

  @Test
  void updateContentOmitsResetToDefaults() throws Exception {
    // updateContent fully replaces content; omitted fields use defaults, not prior values.
    String json =
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_snapshot_expiration\","
            + "\"newContent\":{\"olderThanDays\":30}}";
    PolicyUpdateRequest.UpdatePolicyContentRequest update =
        JsonUtils.objectMapper()
            .readValue(json, PolicyUpdateRequest.UpdatePolicyContentRequest.class);
    update.validate();
    Assertions.assertEquals(
        PolicyContents.icebergSnapshotExpiration(
            30L,
            IcebergSnapshotExpirationContent.DEFAULT_RETAIN_LAST,
            IcebergSnapshotExpirationContent.DEFAULT_STREAM_RESULTS),
        DTOConverters.fromDTO(update.getNewContent()));
  }

  @Test
  void responseRoundTripRetainsTypedContent() throws Exception {
    PolicyContent domain = PolicyContents.icebergSnapshotExpiration(14L, 5L, true);
    PolicyDTO dto =
        PolicyDTO.builder()
            .withName("expire")
            .withPolicyType("system_iceberg_snapshot_expiration")
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

  @ParameterizedTest
  @CsvSource({
    "olderThanDays,14.5",
    "olderThanDays,\"14\"",
    "olderThanDays,true",
    "retainLast,1.0",
    "retainLast,\"1\"",
    "retainLast,false",
    "streamResults,0",
    "streamResults,1",
    "streamResults,\"true\"",
    "streamResults,\"false\"",
    "streamResults,[]",
    "streamResults,{}"
  })
  void rejectInvalidValuesInCreateAndUpdate(String field, String value) {
    String content = "{\"" + field + "\":" + value + "}";
    String create =
        "{\"name\":\"expire\",\"policyType\":\"system_iceberg_snapshot_expiration\","
            + "\"enabled\":true,\"content\":"
            + content
            + "}";
    String update =
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_snapshot_expiration\","
            + "\"newContent\":"
            + content
            + "}";
    Assertions.assertThrows(
        JsonProcessingException.class,
        () -> JsonUtils.objectMapper().readValue(create, PolicyCreateRequest.class));
    Assertions.assertThrows(
        JsonProcessingException.class,
        () ->
            JsonUtils.objectMapper()
                .readValue(update, PolicyUpdateRequest.UpdatePolicyContentRequest.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void booleanStreamResultsValues(boolean streamResults) throws Exception {
    PolicyContentDTO.IcebergSnapshotExpirationContentDTO dto =
        JsonUtils.objectMapper()
            .readValue(
                "{\"streamResults\":" + streamResults + "}",
                PolicyContentDTO.IcebergSnapshotExpirationContentDTO.class);
    dto.validate();
    Assertions.assertEquals(
        PolicyContents.icebergSnapshotExpiration(null, null, streamResults),
        DTOConverters.fromDTO(dto));
  }

  @Test
  void createAndUpdateValidateRetentionUpperBoundary() throws Exception {
    long maximum = IcebergSnapshotExpirationContent.MAX_OLDER_THAN_DAYS;
    for (long days : new long[] {maximum, maximum + 1, Long.MAX_VALUE}) {
      String content = "{\"olderThanDays\":" + days + "}";
      PolicyCreateRequest create =
          JsonUtils.objectMapper()
              .readValue(
                  "{\"name\":\"expire\",\"policyType\":\"system_iceberg_snapshot_expiration\",\"content\":"
                      + content
                      + "}",
                  PolicyCreateRequest.class);
      PolicyUpdateRequest update =
          JsonUtils.objectMapper()
              .readValue(
                  "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_snapshot_expiration\",\"newContent\":"
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
