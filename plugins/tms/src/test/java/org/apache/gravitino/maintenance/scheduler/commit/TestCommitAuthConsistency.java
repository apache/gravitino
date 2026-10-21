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
package org.apache.gravitino.maintenance.scheduler.commit;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCommitAuthConsistency {

  @Test
  void testAgreeingAuthPasses() {
    Config config = new Config(false) {};
    CommitChainMember a = member(1L, Map.of("rest.auth.type", "none"), Map.of("auth_type", "none"));
    CommitChainMember b = member(2L, Map.of("rest.auth.type", "none"), Map.of("auth_type", "none"));
    Assertions.assertDoesNotThrow(
        () -> CommitAuthConsistency.requireConsistent(List.of(a, b), config));
  }

  @Test
  void testMismatchingGravitinoAuthFails() {
    Config config = new Config(false) {};
    CommitChainMember a = member(1L, Map.of(), Map.of("auth_type", "none"));
    CommitChainMember b = member(2L, Map.of(), Map.of("auth_type", "simple", "username", "u"));
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> CommitAuthConsistency.requireConsistent(List.of(a, b), config));
    Assertions.assertTrue(ex.getMessage().contains("gravitinoAuth mismatch"));
  }

  @Test
  void testMismatchingIrcAuthFails() {
    Config config = new Config(false) {};
    CommitChainMember a = member(1L, Map.of("rest.auth.type", "none"), Map.of());
    CommitChainMember b = member(2L, Map.of("rest.auth.type", "basic"), Map.of());
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> CommitAuthConsistency.requireConsistent(List.of(a, b), config));
    Assertions.assertTrue(ex.getMessage().contains("ircAuth mismatch"));
  }

  @Test
  void testClusterIrcAuthOverlayMakesPoliciesAgree() {
    Config config = new Config(false) {};
    config.loadFromMap(Map.of("gravitino.maintenance.ircAuth.rest.auth.type", "basic"), t -> true);
    // Policies disagree in content, but cluster ircAuth wins on overlay.
    CommitChainMember a = member(1L, Map.of("rest.auth.type", "none"), Map.of());
    CommitChainMember b = member(2L, Map.of("rest.auth.type", "oauth"), Map.of());
    Assertions.assertDoesNotThrow(
        () -> CommitAuthConsistency.requireConsistent(List.of(a, b), config));
  }

  private static CommitChainMember member(
      long id, Map<String, String> jobOptions, Map<String, String> updaterOptions) {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(id)
            .withName("p-" + id)
            .withNamespace(NamespaceUtil.ofPolicy("ml"))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withContent(
                PolicyContents.icebergDataCompaction(
                    IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
                    IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
                    IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
                    IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
                    IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
                    "binpack",
                    "",
                    Map.of(),
                    null,
                    true,
                    null,
                    jobOptions,
                    updaterOptions))
            .withEnabled(true)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
            .build();
    return new CommitChainMember(
        Policy.BuiltInType.ICEBERG_COMPACTION, policy, "rewrite_data_files");
  }
}
