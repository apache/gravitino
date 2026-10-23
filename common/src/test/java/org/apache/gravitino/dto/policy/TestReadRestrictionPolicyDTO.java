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
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.policy.ColumnMaskContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.RowFilterContent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestReadRestrictionPolicyDTO {

  @Test
  void testRowFilterPolicyRoundTrip() throws JsonProcessingException {
    String expression =
        "filter := col(\"region\") == \"US\" if is_group_member(\"auditors\") "
            + "else := col(\"owner\") == session_user()";
    RowFilterContent content = PolicyContents.rowFilter(expression);
    PolicyDTO policy =
        PolicyDTO.builder()
            .withName("owner-filter")
            .withPolicyType("system_row_filter")
            .withEnabled(true)
            .withContent(DTOConverters.toDTO(content))
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(policy);
    PolicyDTO restored = JsonUtils.objectMapper().readValue(json, PolicyDTO.class);

    Assertions.assertEquals(policy, restored);
    Assertions.assertInstanceOf(PolicyContentDTO.RowFilterContentDTO.class, restored.content());
    Assertions.assertTrue(json.contains("\"expression\":\"filter :="));
    Assertions.assertFalse(json.contains("\"rules\""));
    Assertions.assertEquals(content, DTOConverters.fromDTO(restored.content()));
    Assertions.assertDoesNotThrow(restored.content()::validate);
  }

  @Test
  void testRowFilterCreateRequestRoundTrip() throws JsonProcessingException {
    String json =
        "{"
            + "\"name\":\"region-filter\","
            + "\"policyType\":\"system_row_filter\","
            + "\"enabled\":true,"
            + "\"content\":{\"expression\":"
            + "\"filter := col(\\\"region\\\") == \\\"US\\\" if "
            + "is_group_member(\\\"auditors\\\") else := false\"}}";

    PolicyCreateRequest request =
        JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class);
    Assertions.assertInstanceOf(
        PolicyContentDTO.RowFilterContentDTO.class, request.getPolicyContent());
    Assertions.assertDoesNotThrow(request::validate);

    RowFilterContent content = (RowFilterContent) DTOConverters.fromDTO(request.getPolicyContent());
    Assertions.assertEquals(
        "filter := col(\"region\") == \"US\" if is_group_member(\"auditors\") else := false",
        content.expression());
  }

  @Test
  void testColumnMaskCreateRequestRoundTrip() throws JsonProcessingException {
    String json =
        "{"
            + "\"name\":\"pii-mask\","
            + "\"policyType\":\"system_column_mask\","
            + "\"enabled\":true,"
            + "\"content\":{\"expression\":"
            + "\"mask := action(\\\"show-last-4\\\") if "
            + "is_group_member(\\\"pii_unmasked\\\") "
            + "else := action(\\\"mask-alphanum\\\")\"}}";

    PolicyCreateRequest request =
        JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class);
    Assertions.assertInstanceOf(
        PolicyContentDTO.ColumnMaskContentDTO.class, request.getPolicyContent());
    Assertions.assertDoesNotThrow(request::validate);

    ColumnMaskContent content =
        (ColumnMaskContent) DTOConverters.fromDTO(request.getPolicyContent());
    Assertions.assertEquals(
        "mask := action(\"show-last-4\") if is_group_member(\"pii_unmasked\") "
            + "else := action(\"mask-alphanum\")",
        content.expression());
  }

  @Test
  void testUpdateRequestsUseEachPolicyType() throws JsonProcessingException {
    assertUpdateRoundTrip(
        "system_row_filter",
        PolicyContents.rowFilter(
            "filter := col(\"region\") == \"US\" if session_user() == \"alice\" "
                + "else := col(\"owner\") == session_user()"));
    assertUpdateRoundTrip(
        "system_column_mask",
        PolicyContents.columnMask(
            "mask := action(\"show-last-4\") if session_user() == \"alice\" "
                + "else := action(\"replace-with-null\")"));
  }

  @Test
  void testStoredContentRoundTripUsesEachBuiltInContentClass() throws JsonProcessingException {
    RowFilterContent rowFilter =
        PolicyContents.rowFilter(
            "filter := col(\"region\") == \"US\" if is_group_member(\"analysts\") "
                + "else := col(\"owner\") == session_user()");
    ColumnMaskContent columnMask =
        PolicyContents.columnMask(
            "mask := action(\"show-last-4\") if is_group_member(\"analysts\") "
                + "else := action(\"replace-with-null\")");

    PolicyContent restoredRowFilter =
        JsonUtils.anyFieldMapper()
            .readValue(
                JsonUtils.anyFieldMapper().writeValueAsString(rowFilter),
                Policy.BuiltInType.ROW_FILTER.contentClass());
    PolicyContent restoredColumnMask =
        JsonUtils.anyFieldMapper()
            .readValue(
                JsonUtils.anyFieldMapper().writeValueAsString(columnMask),
                Policy.BuiltInType.COLUMN_MASK.contentClass());

    Assertions.assertEquals(rowFilter, restoredRowFilter);
    Assertions.assertEquals(columnMask, restoredColumnMask);
    Assertions.assertDoesNotThrow(restoredRowFilter::validate);
    Assertions.assertDoesNotThrow(restoredColumnMask::validate);
  }

  @Test
  void testRejectsLegacyRuleShape() {
    String rowFilterWithRules =
        "{"
            + "\"name\":\"invalid-filter\","
            + "\"policyType\":\"system_row_filter\","
            + "\"enabled\":true,"
            + "\"content\":{\"rules\":[{\"expression\":\"filter := true\"}]}"
            + "}";
    String columnMaskWithRules =
        "{"
            + "\"name\":\"invalid-mask\","
            + "\"policyType\":\"system_column_mask\","
            + "\"enabled\":true,"
            + "\"content\":{\"rules\":[{\"action\":\"show-first-4\"}]}"
            + "}";

    assertLegacyRuleShapeRejected(rowFilterWithRules);
    assertLegacyRuleShapeRejected(columnMaskWithRules);
  }

  @Test
  void testRejectsMissingOrBlankExpressions() {
    PolicyContentDTO.RowFilterContentDTO missingRowFilter =
        PolicyContentDTO.RowFilterContentDTO.builder().build();
    PolicyContentDTO.ColumnMaskContentDTO missingColumnMask =
        PolicyContentDTO.ColumnMaskContentDTO.builder().build();
    PolicyContentDTO.RowFilterContentDTO blankRowFilter =
        PolicyContentDTO.RowFilterContentDTO.builder().withExpression(" ").build();
    PolicyContentDTO.ColumnMaskContentDTO blankColumnMask =
        PolicyContentDTO.ColumnMaskContentDTO.builder().withExpression("\n").build();

    Assertions.assertThrows(IllegalArgumentException.class, missingRowFilter::validate);
    Assertions.assertThrows(IllegalArgumentException.class, missingColumnMask::validate);
    Assertions.assertThrows(IllegalArgumentException.class, blankRowFilter::validate);
    Assertions.assertThrows(IllegalArgumentException.class, blankColumnMask::validate);
  }

  private static void assertUpdateRoundTrip(String policyType, PolicyContent content)
      throws JsonProcessingException {
    PolicyUpdateRequest.UpdatePolicyContentRequest request =
        new PolicyUpdateRequest.UpdatePolicyContentRequest(
            policyType, DTOConverters.toDTO(content));
    String json = JsonUtils.objectMapper().writeValueAsString(request);
    PolicyUpdateRequest.UpdatePolicyContentRequest restored =
        JsonUtils.objectMapper()
            .readValue(json, PolicyUpdateRequest.UpdatePolicyContentRequest.class);

    Assertions.assertEquals(request, restored);
    Assertions.assertEquals(policyType, restored.getPolicyType());
    Assertions.assertEquals(content, DTOConverters.fromDTO(restored.getNewContent()));
  }

  private static void assertLegacyRuleShapeRejected(String json) {
    UnrecognizedPropertyException exception =
        Assertions.assertThrows(
            UnrecognizedPropertyException.class,
            () -> JsonUtils.objectMapper().readValue(json, PolicyCreateRequest.class));
    Assertions.assertEquals("rules", exception.getPropertyName());
    Assertions.assertTrue(exception.getMessage().contains("rules"));
  }
}
