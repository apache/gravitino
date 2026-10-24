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
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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

  @ParameterizedTest
  @CsvSource({
    "use_caching,0",
    "use_caching,1",
    "use_caching,1.0",
    "use_caching,\"true\"",
    "use_caching,\"false\"",
    "use_caching,[]",
    "use_caching,{}",
    "spec_id,1.5",
    "spec_id,2147483648",
    "spec_id,true",
    "spec_id,\"1\"",
    "manifest_count_critical,500.5",
    "manifest_count_critical,9223372036854775808",
    "manifest_count_warning,100.5",
    "manifest_count_warning,false",
    "avg_manifest_size_threshold_bytes,4096.5",
    "avg_manifest_size_threshold_bytes,\"4096\""
  })
  void testRejectInvalidValuesInCreateAndUpdate(String field, String value) {
    String content = "{\"" + field + "\":" + value + "}";
    String create =
        "{\"name\":\"rewrite\",\"policyType\":\"system_iceberg_rewrite_manifests\","
            + "\"enabled\":true,\"content\":"
            + content
            + "}";
    String update =
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_rewrite_manifests\","
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
  void testBooleanCachingValues(boolean useCaching) throws Exception {
    PolicyContentDTO.IcebergRewriteManifestsContentDTO dto =
        JsonUtils.objectMapper()
            .readValue(
                "{\"use_caching\":" + useCaching + "}",
                PolicyContentDTO.IcebergRewriteManifestsContentDTO.class);
    dto.validate();
    Assertions.assertEquals(
        PolicyContents.icebergRewriteManifests(null, null, null, null, useCaching),
        DTOConverters.fromDTO(dto));
  }

  @Test
  void testNumericBoundsAndNullDefaults() throws Exception {
    PolicyContentDTO.IcebergRewriteManifestsContentDTO bounds =
        JsonUtils.objectMapper()
            .readValue(
                "{\"spec_id\":2147483647,\"manifest_count_critical\":9223372036854775807}",
                PolicyContentDTO.IcebergRewriteManifestsContentDTO.class);
    bounds.validate();
    Assertions.assertEquals(Integer.MAX_VALUE, bounds.specId());
    Assertions.assertEquals(Long.MAX_VALUE, bounds.manifestCountCritical());
    PolicyContentDTO.IcebergRewriteManifestsContentDTO defaults =
        JsonUtils.objectMapper()
            .readValue(
                "{\"spec_id\":null,\"manifest_count_critical\":null,\"manifest_count_warning\":null,"
                    + "\"avg_manifest_size_threshold_bytes\":null,\"use_caching\":null}",
                PolicyContentDTO.IcebergRewriteManifestsContentDTO.class);
    defaults.validate();
    Assertions.assertEquals(
        PolicyContents.icebergRewriteManifests(), DTOConverters.fromDTO(defaults));
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
