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
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestCompactionPolicyStrictTypes {
  @ParameterizedTest
  @CsvSource({
    "maxPartitionNum,7.9",
    "maxPartitionNum,\"50\"",
    "maxPartitionNum,true",
    "minDataFileMse,1.5",
    "minDeleteFileNumber,\"2\"",
    "dataFileMseWeight,0.5",
    "deleteFileNumberWeight,3.0",
    "minIntervalMs,3600.5",
    "minIntervalMs,\"100\"",
    "commitTrigger,\"true\"",
    "commitTrigger,1",
    "rewriteStrategy,1",
    "rewriteStrategy,true",
    "sortOrder,2",
    "sortOrder,true",
    "timeTrigger,1",
    "timeTrigger,true",
    "rewriteOptions,1",
    "jobOptions,1",
    "updaterOptions,true"
  })
  void rejectInvalidValuesInCreateAndUpdate(String field, String value) {
    assertRejected("{\"" + field + "\":" + value + "}");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"rewriteOptions\":{\"target-file-size-bytes\":134217728}}",
        "{\"rewriteOptions\":{\"partial-progress-enabled\":true}}",
        "{\"jobOptions\":{\"k\":1}}",
        "{\"updaterOptions\":{\"metalake\":true}}"
      })
  void rejectNonStringMapEntryValues(String content) {
    assertRejected(content);
  }

  private static void assertRejected(String content) {
    String create =
        "{\"name\":\"compact\",\"policyType\":\"system_iceberg_compaction\","
            + "\"enabled\":true,\"content\":"
            + content
            + "}";
    String update =
        "{\"@type\":\"updateContent\",\"policyType\":\"system_iceberg_compaction\","
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
}
